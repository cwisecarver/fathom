defmodule Fathom.ShardExecutorOpenErrorTest do
  @moduledoc """
  Every typed open refusal is classified (expert review 2026-11-07, follow-up from the batch-D fix).

  `ShardExecutor.open_error/1` is the one place an open's `{:error, reason}` becomes the status a
  client sees. Four refusals the open path grew (`{:pull_failed, _}` and the `:pull_temp_missing`,
  `{:local_stat_failed, _}` and `:sidecar_corrupt` reasons it carries) had no clause, so they fell to
  the catch-all and a client got a 500 `FILO_OPEN_FAILED` for something that clears on a retry —
  an SDK treats a 500 as permanent.

  ## The rule

    * a refusal that a retry can clear (a vanished or unreadable pull temp, a pull timeout/crash, a
      stat or claim that failed, a heartbeat that is down) is the retryable 503 `FILO_UNAVAILABLE`
      the other transient refusals use (`FILO_RECOVERING` for the recovery refusal window);
    * a refusal that needs an operator (a warm file whose provenance cannot be trusted) is a stable,
      NON-retryable `FILO_SHARD_UNRECOVERABLE` 500 — distinct from the unclassified
      `FILO_OPEN_FAILED`, so an SDK and an alert can tell "will not heal" from "unknown";
    * `{:pull_failed, reason}` takes the class of its `reason`; an unlisted `reason` is retryable
      (it is by construction "the pull or promotion did not finish").

  ## Drift guard

  The second test reads the sources that mint these reasons and fails when a literal reason tag is
  neither in the table below nor in the (commented) set of tags that never reach a client. Adding a
  refusal without classifying it fails here, instead of shipping as a 500.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Fathom.ShardExecutor
  alias Filo.Error

  @retryable {503, "FILO_UNAVAILABLE"}
  @recovering {503, "FILO_RECOVERING"}
  @operator {500, "FILO_SHARD_UNRECOVERABLE"}

  # reason → {status, code}. `tag` is the reason's literal head, for the drift guard.
  @classified [
    # Reasons `Shard.handle_continue(:open)` stops with directly.
    {{:local_stat_failed, :eio}, @retryable},
    {{:retained_claim_failed, :eacces}, @retryable},
    {{:retained_claim_unavailable, {:timeout, {GenServer, :call, []}}}, @retryable},
    # `{:pull_failed, reason}`: what the pull / promotion / sidecar steps return.
    {{:pull_failed, :pull_temp_missing}, @retryable},
    {{:pull_failed, {:pull_temp_unreadable, :eio}}, @retryable},
    {{:pull_failed, :pull_timeout}, @retryable},
    {{:pull_failed, {:pull_crashed, :killed}}, @retryable},
    {{:pull_failed, {:pull_exception, %RuntimeError{message: "x"}}}, @retryable},
    {{:pull_failed, {:pull_exit, :closed}}, @retryable},
    {{:pull_failed, {:etag_unavailable, :timeout}}, @retryable},
    {{:pull_failed, {:sidecar_write_failed, :enospc}}, @retryable},
    {{:pull_failed, {:s3_get_status, 503}}, @retryable},
    {{:pull_failed, :never_heard_of_it}, @retryable},
    {{:pull_failed, :sidecar_corrupt}, @operator},
    {{:pull_failed, :no_provenance}, @operator},
    # The node's heartbeat is configured but cannot answer (expert review 2026-11-07 #59).
    {{:heartbeat_unavailable, :down}, @retryable},
    {{:heartbeat_unavailable, :booting}, @retryable},
    # The recovery refusal window (A2 floor / failed pull, `Recovery.error_window/3`).
    {{:open_failed,
      %Fathom.Shard.Replication.Recovery.UnderQuorum{
        shard_id: "s",
        answered: 0,
        needed: 2,
        waited_ms: 0
      }}, @recovering},
    # Already classified before this change: kept in the table so the table is the whole picture.
    {{:shard_held, "n#1", 0}, {503, "FILO_SHARD_HELD"}},
    {{:lease_unavailable, :closed}, @retryable},
    {{:open_failed, {:exit, :noproc}}, @retryable},
    {:timeout, @retryable},
    {:normal, @retryable},
    {:unavailable, @retryable},
    {:shard_tombstoned, {410, "FILO_TENANT_DELETED"}},
    {:shard_suspended, {403, "FILO_TENANT_SUSPENDED"}}
  ]

  for {reason, {status, code}} <- @classified do
    test "#{inspect(reason, limit: 4, printable_limit: 40)} -> #{status} #{code}" do
      capture_log(fn ->
        assert %Error{status: unquote(status), code: unquote(code)} =
                 ShardExecutor.open_error(unquote(Macro.escape(reason)))
      end)
    end
  end

  test "the operator-class refusal is not retryable and carries no internal detail" do
    {error, _log} = with_log(fn -> ShardExecutor.open_error({:pull_failed, :sidecar_corrupt}) end)

    assert error.status == 500
    assert error.code == "FILO_SHARD_UNRECOVERABLE"
    refute error.message =~ "sidecar"
    refute error.message =~ "provenance"
  end

  # Tags that reach `open_error/1` only through a clause that already existed, or never reach a
  # client at all (internal results the open path consumes). Each has a reason; keep them honest.
  @not_client_facing %{
    # Returned by `Materializer`/`Shard` helpers that the open path folds into `{:pull_failed, _}`
    # or consumes itself, so the CLASSIFIED tag is the wrapper, not these.
    "etag_unavailable" => "inside pull_failed",
    "pull_temp_unreadable" => "inside pull_failed",
    "pull_timeout" => "inside pull_failed",
    "pull_crashed" => "inside pull_failed",
    "pull_exception" => "inside pull_failed",
    "pull_exit" => "inside pull_failed",
    "sidecar_write_failed" => "inside pull_failed",
    "pull_temp_missing" => "inside pull_failed",
    "sidecar_corrupt" => "inside pull_failed",
    "no_provenance" => "inside pull_failed",
    # `PromoteOnOpen.promote_replica` puts the pre-promotion db aside; the failure RAISES inside the
    # lease-holding region, so the client sees `{:open_failed, exception}` (a deliberate 500: a raw
    # exception is a code/data fault, see open_error/1).
    "aside_failed" => "raised, surfaces as {:open_failed, exception}",
    # A `File.rename/2` result MATCHED in promote_on_open (`{:error, :enoent} ->`), not minted.
    "enoent" => "a matched File result"
  }

  test "drift guard: every literal open-refusal tag the sources mint is classified" do
    sources = [
      "lib/fathom/shard/materializer.ex",
      "lib/fathom/shard/promote_on_open.ex",
      "lib/fathom/shard/retained.ex"
    ]

    tags =
      for src <- sources,
          line <- src |> File.read!() |> String.split("\n"),
          not String.starts_with?(String.trim(line), "#"),
          [_, tag] <- Regex.scan(~r/\{:error, \{?:([a-z_]+)/, line),
          into: MapSet.new(),
          do: tag

    classified_heads =
      for {reason, _} <- @classified, into: MapSet.new() do
        head = if is_tuple(reason), do: elem(reason, 0), else: reason

        case head do
          :pull_failed -> reason |> elem(1) |> then(&if(is_tuple(&1), do: elem(&1, 0), else: &1))
          other -> other
        end
        |> to_string()
      end

    unclassified =
      tags
      |> Enum.reject(&(&1 in classified_heads))
      |> Enum.reject(&Map.has_key?(@not_client_facing, &1))
      |> Enum.sort()

    assert MapSet.size(tags) > 5,
           "the scan found #{inspect(tags)}: the regex probably stopped matching"

    assert unclassified == [],
           """
           These {:error, <tag>…} reasons are minted on the open path but neither classified in
           @classified nor listed in @not_client_facing:

             #{Enum.join(unclassified, ", ")}

           An unclassified refusal reaches the client as a 500 FILO_OPEN_FAILED. Add a
           ShardExecutor.open_error/1 clause (retryable 503 if a retry can clear it, a stable
           non-retryable code otherwise) and a row in @classified.
           """
  end
end
