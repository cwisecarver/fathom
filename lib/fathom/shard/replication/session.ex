defmodule Fathom.Shard.Replication.Session do
  @moduledoc """
  One replication session per shard — the commit-path integration for A2.
  See `docs/a2-quorum-replication.md`.

  Owns the shard's `Primary` state (`{wal_gen, salt1, offset}`) and is the **serialization point**
  for shipping. One process per shard, so two streams that commit back to back cannot both compute
  a delta from the same stale offset and ship overlapping ranges.

  ## Why this is not in `Fathom.Shard`

  The coordinator would have been the obvious home — it is already one process per shard and
  already owns the lease this module fences with. It is also ~3000 lines of intricate
  lease/fence/flush state machine, and a synchronous multi-millisecond network wait inside its
  mailbox would sit in front of every checkout and every durability flush for that shard. Keeping
  the wait in a separate process means a slow follower delays commits for its shard and nothing
  else.

  ## The gate, and what a tenant sees when the quorum fails

  Off unless `:replication_enabled`. When on, a committing statement does not return to the client
  until `q` followers have acked.

  **The local commit has already happened by then** — SQLite committed before the WAL could be
  read, and there is no un-commit. So a quorum failure returns an error for a write that IS durable
  locally and WILL reach S3 on the next flush. That is deliberate and it is the honest direction:
  the client asked for a quorum-durable write and did not get one, so it must not be told it did.
  The cost is an **at-least-once** hazard — a client that retries may apply the write twice — which
  is the same hazard any commit-ack-lost path already has (`docs/durability.md`), not a new one.

  Reporting success instead would be worse in the way that matters: it would make the failure
  invisible exactly when the data is least protected.
  """
  use GenServer

  require Logger

  alias Fathom.Shard.Replication
  alias Fathom.Shard.Replication.Primary
  alias Fathom.Shard.Replication.Protocol.Push
  alias Fathom.Shard.Replication.Protocol.Seed
  alias Fathom.Shard.Replication.Shipper
  alias Fathom.Shard.Replication.Wal

  @registry Fathom.Shard.Replication.SessionRegistry

  # ------------------------------------------------------------------------------------------
  # api
  # ------------------------------------------------------------------------------------------

  @doc "Whether commit-path replication is on. Off by default."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:fathom, :replication_enabled, false)

  @doc """
  Replicate everything committed to `wal_path` since the last successful ship.

  Synchronous by design: this is the quorum wait, and the point of A2 is that the client's success
  is gated on it. Returns `:ok` when nothing needed shipping, too.
  """
  @spec commit(String.t(), Path.t(), pid()) :: :ok | {:error, term()}
  def commit(shard_id, wal_path, coordinator) do
    with {:ok, pid} <- ensure_started(shard_id, coordinator) do
      GenServer.call(pid, {:commit, wal_path}, timeout())
    end
  catch
    :exit, reason -> {:error, {:session_down, reason}}
  end

  @doc false
  def child_spec(opts) do
    %{
      id: {__MODULE__, opts[:shard_id]},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  def start_link(opts) do
    shard_id = Keyword.fetch!(opts, :shard_id)
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {@registry, shard_id}})
  end

  @doc """
  Forget a shard's replication state — used when it drains, and by tests.

  A no-op when replication is not running. The registry only exists under `Fleet`, so with the
  feature off (or already shut down) there is nothing to look up, and a caller tidying up should
  not have to know which. Raising here made teardown fail in tests that had stopped `Fleet` first.
  """
  @spec stop(String.t()) :: :ok
  def stop(shard_id) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] -> GenServer.stop(pid, :normal)
      [] -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp ensure_started(shard_id, coordinator) do
    case Registry.lookup(@registry, shard_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               Fathom.Shard.Replication.SessionSupervisor,
               {__MODULE__, shard_id: shard_id, coordinator: coordinator}
             ) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp timeout, do: Application.get_env(:fathom, :replication_timeout_ms, 5_000)

  # ------------------------------------------------------------------------------------------
  # server
  # ------------------------------------------------------------------------------------------

  @impl true
  def init(opts) do
    coordinator = Keyword.fetch!(opts, :coordinator)

    # The cached epoch must never outlive the ownership it describes. Monitoring the coordinator
    # means a steal — which stops it — also stops this session, so the next commit starts a fresh
    # one and re-reads the epoch. Without this, a deposed node would keep shipping under its old
    # epoch and rely entirely on the follower to notice.
    Process.monitor(coordinator)

    {:ok,
     %{
       shard_id: Keyword.fetch!(opts, :shard_id),
       coordinator: coordinator,
       epoch: nil,
       # The WAL this shard commits through, remembered from the last commit. A follower's reply can
       # arrive long after the call that provoked it returned, and acting on `:unknown_shard` means
       # reading the shard's files — so the handler needs the path without a caller to supply it.
       # `nil` until the first commit, which is also the first moment a reply can exist.
       wal_path: nil,
       # PER-FOLLOWER position: %{shipper => %{wal_gen, salt1, offset}}.
       #
       # A single shared offset was the original shape and it made catch-up impossible: followers
       # seeded at different times sit at different positions, so one delta cannot be correct for
       # all of them. A replica that fell behind was sent bytes starting past where it actually
       # was, rejected them forever, and stayed silently un-replicated while the shard reported a
       # healthy quorum. Each follower now gets the delta from ITS own position.
       followers: %{},
       # What each follower WOULD be at if it accepts the push currently outstanding to it:
       # %{shipper => %{wal_gen, salt1, offset}}.
       #
       # A reply is only meaningful against the push that provoked it, and the two arrive at
       # different times: `ship_quorum/3` returns at the Q-th ack, so the rest answer into this
       # mailbox afterwards and are read by a later `drain_late_replies/2` with no plan in scope.
       # Recording the expectation at send time is what lets that drain act on a straggler's answer
       # at all — an ack advances the follower to this position, and an `:offset_mismatch` keeps the
       # generation and salt from the same record while taking the offset the follower reported.
       inflight: %{},
       # Shippers with a seed in flight. A seed can be a whole database, so starting a second one
       # for the same follower because the next commit also saw :unknown_shard would multiply a
       # large transfer by the write rate.
       seeding: MapSet.new()
     }}
  end

  # Deliberately `case`, not `with`/`else`. The first version used `with {:ok, state} <- ...` and
  # then referenced `state` in the else branch — where it is still the OUTER binding, so the epoch
  # `with_epoch/1` had just resolved was silently `nil` again. The seed task then crashed encoding
  # `nil::64` inside a `Task.start`, which swallows it, and the only symptom was followers that
  # were never seeded. Nesting keeps the epoch-bearing state in scope where it is used.
  @impl true
  def handle_call({:commit, wal_path}, _from, state) do
    case with_epoch(state) do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:ok, state} ->
        # Drain first: a straggler's reply that landed while the PREVIOUS call was still running has
        # not reached `handle_info/2` yet, and leaving it would let the next `collect/4` mistake it
        # for an answer to the push about to be sent.
        state = drain_late_replies(%{state | wal_path: wal_path}, wal_path)

        case ship(state, wal_path, state.epoch) do
          {:ok, new_state, rejects} ->
            new_state = start_seeds(new_state, wal_path, rejects)
            {:reply, :ok, drain_late_replies(new_state, wal_path)}

          :nothing ->
            {:reply, :ok, state}

          # `new_state` already has every rejecter's position corrected from what IT reported, so
          # the next commit plans a catch-up delta from where each actually is.
          #
          # A follower answering :unknown_shard has never been seeded, which is not a fault to
          # alert on but the signal to send it a base copy — started OUT OF BAND. A seed can be a
          # whole database and this commit is a tenant waiting on a write, so blocking it on a
          # multi-megabyte transfer would be the worst possible place to put that cost. The commit
          # still fails honestly, and the next one finds the follower ready.
          {:error, {:no_quorum, why}, new_state, rejects} ->
            {:reply, {:error, {:no_quorum, why}}, start_seeds(new_state, wal_path, rejects)}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl true
  def handle_info({:seeded, shipper, result}, state) do
    state = %{state | seeding: MapSet.delete(state.seeding, shipper)}

    case result do
      {:ok, at} ->
        # Record where the seed left THIS follower. Without it the primary's next delta for this
        # follower starts from wherever it last thought it was — nowhere, for a fresh one — and the
        # follower rejects every frame while holding perfectly good bytes. Per-follower precisely
        # because seeds land at different times and therefore different offsets.
        Logger.info("replication seeded a follower for #{state.shard_id} at #{inspect(at)}")
        key = resolve(shipper)

        # Drop any outstanding expectation for this follower along with it. A seed replaces
        # everything the follower held, so a push that was in flight when it started describes a
        # position that no longer exists; letting its reply land afterwards would overwrite the seed
        # with a stale one.
        {:noreply,
         %{
           state
           | followers: Map.put(state.followers, key, at),
             inflight: Map.delete(state.inflight, key)
         }}

      {:error, r} ->
        Logger.warning("replication seed failed for #{state.shard_id}: #{inspect(r)}")
        {:noreply, state}
    end
  end

  # A follower's reply that arrives while the session is IDLE — which is where almost all of them
  # arrive. `ship_quorum/3` returns at the Q-th ack, the call returns, and the stragglers answer
  # into a mailbox nobody is reading; only replies that land *during* a call are seen by
  # `drain_late_replies/2`. Before these clauses existed the catch-all below swallowed them, and the
  # consequences were exactly the ones that made this refactor necessary:
  #
  #   * the follower outside the quorum was never advanced, so the next commit planned a delta it
  #     had already applied, it refused, and it stayed a full delta behind — permanently, if the
  #     tenant stopped writing;
  #   * an `:unknown_shard` from a never-seeded follower was dropped, so whether it was EVER seeded
  #     came down to whether its reply happened to beat the quorum.
  #
  # Both look like replication bugs from the outside and neither is: the answer was received,
  # correct, and thrown away.
  @impl true
  def handle_info({:repl_reply, from, {:ack, _shard, next}}, state) do
    {:noreply, settle_late_ack(state, from, next)}
  end

  def handle_info({:repl_reply, from, {:reject, _shard, reason, follower_offset}}, state) do
    rejects = [{from, reason, follower_offset}]
    {:noreply, seed_if_possible(reconcile(state, rejects), rejects)}
  end

  def handle_info({:DOWN, _ref, :process, coordinator, _reason}, %{coordinator: coordinator} = s) do
    # The shard moved or drained. Our offset describes a WAL this node no longer owns.
    {:stop, :normal, s}
  end

  def handle_info(_, state), do: {:noreply, state}

  # A reply cannot exist before the commit that provoked it, so `wal_path` is always set by the time
  # this runs. Matching on it anyway keeps a reply that somehow arrived first from crashing the
  # session on a nil path — losing a straggler's seed is recoverable, losing the session is not.
  defp seed_if_possible(%{wal_path: nil} = state, _rejects), do: state
  defp seed_if_possible(state, rejects), do: start_seeds(state, state.wal_path, rejects)

  # Read the lease epoch once. `{:error, :no_lease}` must NOT be shipped past — frames from a node
  # without a lease are exactly what the follower's epoch check refuses.
  defp with_epoch(%{epoch: e} = state) when is_integer(e), do: {:ok, state}

  defp with_epoch(state) do
    case Fathom.Shard.epoch(state.coordinator) do
      {:ok, epoch} -> {:ok, %{state | epoch: epoch}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ship(state, wal_path, epoch) do
    case Wal.read(wal_path) do
      {:ok, header} -> ship_planned(state, wal_path, epoch, header)
      {:error, reason} -> read_failed(state, reason)
    end
  end

  # Plan each follower independently, then group by plan so identical ones share a single `pread`
  # and a single encode. In steady state every follower is at the same offset and there is exactly
  # one group; a laggard simply forms a second, larger one.
  #
  # A follower planned `:nothing` is not merely skipped, it is COUNTED. It already holds every byte
  # this commit would replicate, so it satisfies the quorum with no round trip — and dropping it
  # from the tally was a bug in two directions at once. The visible half: `n` shrank to the size of
  # the push list while `q` stayed the configured value, so `Quorum.new/2` raised `q >= n` inside a
  # tenant's commit. The quiet half: a follower that is up to date would have been treated as one
  # that had not answered, so the commit waited on strictly worse replicas than the ones it already
  # had.
  defp ship_planned(state, wal_path, epoch, header) do
    {plans, current} =
      Enum.reduce(shippers(), {[], 0}, fn shipper, {plans, current} ->
        case Primary.plan(Map.get(state.followers, resolve(shipper)), header) do
          :nothing -> {plans, current + 1}
          plan -> {[{shipper, plan} | plans], current}
        end
      end)

    if plans == [] do
      :nothing
    else
      case build_pushes(state, wal_path, epoch, header, plans) do
        {:ok, pushes} -> deliver(state, header, plans, pushes, current)
        {:error, reason} -> read_failed(state, reason)
      end
    end
  end

  defp build_pushes(state, wal_path, epoch, header, plans) do
    # One read per DISTINCT range, not per follower.
    ranges = plans |> Enum.map(fn {_s, {_k, off, len}} -> {off, len} end) |> Enum.uniq()

    payloads =
      Enum.reduce_while(ranges, {:ok, %{}}, fn {off, len}, {:ok, acc} ->
        case Wal.read_delta(wal_path, off, len) do
          {:ok, bin} -> {:cont, {:ok, Map.put(acc, {off, len}, bin)}}
          {:error, r} -> {:halt, {:error, r}}
        end
      end)

    with {:ok, by_range} <- payloads do
      {:ok,
       for {shipper, {kind, off, len}} <- plans do
         {shipper,
          %Push{
            shard_id: state.shard_id,
            epoch: epoch,
            wal_gen: header.ckpt_seq,
            # A reset must arrive at offset 0 or FollowerLog refuses it — the follower has to
            # discard its old generation rather than splice across the seam.
            offset: if(kind == :reset, do: 0, else: off),
            payload: Map.fetch!(by_range, {off, len})
          }}
       end}
    end
  end

  defp deliver(state, header, plans, pushes, current) do
    # Recorded BEFORE anything is sent, because a reply can be observed by a later commit's drain
    # with no plan in scope. See the `inflight` note in `init/1`.
    state = %{state | inflight: Map.merge(state.inflight, expectations(plans, header))}

    case max(quorum() - current, 0) do
      # Followers that are already current carry the quorum on their own. The laggards still get
      # their bytes — skipping them is how a replica falls behind forever — but the tenant does not
      # wait for an ack it does not need.
      0 ->
        Replication.ship_async(pushes)
        {:ok, state, []}

      needed ->
        collect(state, pushes, needed)
    end
  end

  defp collect(state, pushes, needed) do
    case Replication.ship_quorum(pushes, needed, timeout()) do
      {:ok, acked, rejects} ->
        # Advance ONLY the followers that actually ACKED — not everyone the quorum succeeded
        # without. Advancing a rejecter would leave the primary believing it holds bytes it
        # refused, and the next delta would start past a gap only that follower has; it would then
        # reject forever while the shard reported a healthy quorum. Found by the late-joiner test.
        state = Enum.reduce(acked, state, &advance(&2, &1))

        # Rejects are returned on SUCCESS too, and they matter just as much here. A quorum of 2 out
        # of 3 succeeds while the third answers `:unknown_shard`, and that reply is consumed into
        # this list rather than arriving late — so if only the failure path acted on it, whether a
        # straggler ever got seeded would come down to whether its reply beat the quorum. It did
        # not, reliably: the late-joiner test hung here.
        {:ok, reconcile(state, rejects), rejects}

      {:error, {:no_quorum, why, rejects}} ->
        {:error, {:no_quorum, why}, reconcile(state, rejects), rejects}
    end
  end

  defp expectations(plans, header) do
    Map.new(plans, fn {shipper, plan} -> {resolve(shipper), Primary.advance(header, plan)} end)
  end

  # `collect/4` has already checked that this follower reported exactly the position we shipped it,
  # so the recorded expectation IS its new state and there is nothing left to verify. Late acks go
  # through `settle_late_ack/3`, which does that check itself.
  defp advance(state, shipper) do
    key = resolve(shipper)

    case Map.pop(state.inflight, key) do
      {nil, _} -> state
      {pos, rest} -> %{state | followers: Map.put(state.followers, key, pos), inflight: rest}
    end
  end

  # A follower that refused told us where it actually is. Believe it: correcting our record here is
  # what turns the next commit's plan into a catch-up delta from that follower's real position,
  # with no separate retransmission machinery. `:unknown_shard` is deliberately NOT reconciled — it
  # means the follower holds nothing at all, and the answer is a seed, not an offset.
  #
  # The correction is applied to the OUTSTANDING push's expectation, not to whatever we last
  # believed. `FollowerLog` only answers `:offset_mismatch` once it has agreed with the push about
  # the epoch and the generation, so the generation and salt we just shipped are the follower's, and
  # only the offset was wrong. Reading them off the previous record instead meant a follower we had
  # no record for — a late joiner, or one whose ack was lost — had its correction thrown away, and
  # the primary went on planning from a position it had already been told was wrong.
  defp reconcile(state, rejects) do
    Enum.reduce(rejects, state, fn
      {shipper, :offset_mismatch, at}, acc ->
        key = resolve(shipper)

        case Map.pop(acc.inflight, key) do
          {nil, _} ->
            acc

          {pos, rest} ->
            %{acc | followers: Map.put(acc.followers, key, %{pos | offset: at}), inflight: rest}
        end

      _other, acc ->
        acc
    end)
  end

  defp read_failed(state, reason) do
    Logger.warning("replication read failed for #{state.shard_id}: #{inspect(reason)}")
    {:error, reason}
  end

  defp resolve(pid) when is_pid(pid), do: pid
  defp resolve(name), do: Process.whereis(name) || name

  # ------------------------------------------------------------------------------------------
  # seeding
  # ------------------------------------------------------------------------------------------

  # Replies that arrived AFTER the quorum was decided.
  #
  # `ship_quorum/4` returns the moment the outcome is known — at Q acks, or as soon as too few
  # followers remain — and that early return is the entire measured value of a quorum. The cost is
  # that the remaining followers answer into this mailbox afterwards, and those answers matter for
  # two reasons:
  #
  #   1. **They are the only way a straggler gets seeded.** With N=3 and Q=2, a failing commit
  #      reports `:impossible` after just two rejects, so a third unseeded follower is never in the
  #      reject list. Once the other two are seeded the quorum then SUCCEEDS at two acks, and that
  #      follower's `:unknown_shard` is never observed again — it stays permanently un-seeded while
  #      the shard reports a healthy quorum. Silent under-replication, found by the seeding test.
  #   2. **They would otherwise accumulate here forever**, one per straggler per commit, for the
  #      life of the shard.
  #
  # Non-blocking (`after 0`): this runs on the commit path and must never wait for a follower that
  # the quorum already decided not to wait for.
  defp drain_late_replies(state, wal_path) do
    receive do
      # A late ACK is not noise to be thrown away — it is a follower reporting that it holds the
      # bytes we sent it, and the ONLY notification we will ever get. Discarding it left that
      # follower pinned at its previous position, so the next commit planned a delta it had already
      # applied; it refused, and the file diverged from the primary's for a whole round trip while
      # the quorum reported healthy. That is what broke byte-identity on the second commit.
      {:repl_reply, from, {:ack, _shard, next}} ->
        state
        |> settle_late_ack(from, next)
        |> drain_late_replies(wal_path)

      {:repl_reply, from, {:reject, _shard, reason, follower_offset}} ->
        rejects = [{from, reason, follower_offset}]

        state
        |> reconcile(rejects)
        |> start_seeds(wal_path, rejects)
        |> drain_late_replies(wal_path)

      {:repl_reply, _from, _other} ->
        drain_late_replies(state, wal_path)
    after
      0 -> state
    end
  end

  # Unlike the in-quorum path, nothing has vetted this ack yet: `collect/4` compares against what it
  # sent, and it is long gone. So the position is checked against the outstanding expectation here.
  # An ack for anywhere else means the two sides disagree, and believing it would advance the
  # primary past bytes the follower does not hold.
  defp settle_late_ack(state, shipper, next) do
    case Map.get(state.inflight, resolve(shipper)) do
      %{offset: ^next} -> advance(state, shipper)
      _ -> state
    end
  end

  defp start_seeds(state, wal_path, rejects) do
    needs =
      for {shipper, :unknown_shard, _at} <- rejects,
          not MapSet.member?(state.seeding, shipper),
          do: shipper

    Enum.reduce(needs, state, fn shipper, acc ->
      session = self()
      db_path = String.replace_suffix(wal_path, "-wal", "")

      Task.start(fn ->
        send(
          session,
          {:seeded, shipper, do_seed(shipper, acc.shard_id, db_path, wal_path, acc.epoch)}
        )
      end)

      %{acc | seeding: MapSet.put(acc.seeding, shipper)}
    end)
  end

  # Read a CONSISTENT base copy and send it.
  #
  # In WAL mode the `.db` file is only rewritten by a checkpoint, so reading it alongside the `-wal`
  # is safe as long as no checkpoint intervenes. The generation is read before AND after: if it
  # moved, a checkpoint rebuilt the `.db` under us and the two halves are from different points in
  # time — which would hand the follower a database and a WAL whose salts do not match. Retrying is
  # correct and cheap; shipping the inconsistent pair is neither.
  defp do_seed(shipper, shard_id, db_path, wal_path, epoch) do
    with {:ok, before} <- Wal.read(wal_path),
         {:ok, db} <- File.read(db_path),
         {:ok, wal} <- read_wal_bytes(wal_path, before),
         {:ok, after_} <- Wal.read(wal_path),
         :ok <- stable?(before, after_) do
      seed = %Seed{
        shard_id: shard_id,
        epoch: epoch,
        wal_gen: gen_of(before),
        wal_offset: byte_size(wal),
        db: db,
        wal: wal
      }

      Shipper.seed(shipper, seed)

      receive do
        {:repl_reply, ^shipper, {:ack, ^shard_id, _}} ->
          {:ok, %{wal_gen: gen_of(before), salt1: salt_of(before), offset: byte_size(wal)}}

        {:repl_reply, ^shipper, {:reject, ^shard_id, r, _}} ->
          {:error, r}
      after
        30_000 -> {:error, :seed_timeout}
      end
    end
  end

  defp read_wal_bytes(_path, :empty), do: {:ok, <<>>}
  defp read_wal_bytes(path, %{size: size}), do: Wal.read_delta(path, 0, size)

  defp gen_of(:empty), do: 0
  defp gen_of(%{ckpt_seq: seq}), do: seq

  defp salt_of(:empty), do: 0
  defp salt_of(%{salt1: s}), do: s

  # `:empty` on both sides is stable (a shard with no WAL yet). Anything else must match on both
  # the generation and the salt, the same pair `Primary.plan/2` corroborates.
  defp stable?(:empty, :empty), do: :ok
  defp stable?(%{ckpt_seq: g, salt1: s}, %{ckpt_seq: g, salt1: s}), do: :ok
  defp stable?(_, _), do: {:error, :checkpoint_during_seed}

  defp shippers, do: Fathom.Shard.Replication.Fleet.shippers()
  defp quorum, do: Application.get_env(:fathom, :replication_quorum, 2)
end
