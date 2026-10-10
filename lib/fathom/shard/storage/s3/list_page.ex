defmodule Fathom.Shard.Storage.S3.ListPage do
  @moduledoc """
  One parsed ListObjectsV2 response page (expert review 2026-10-10 #25).

  The S3 backend used to scrape these responses with regexes: `<Key>(.*?)</Key>` never decoded
  entities (a key containing `&` comes back as `&amp;`, so a purge looked up and DELETEd a key that
  does not exist), and a page whose continuation token went unseen ended the loop with `:ok` and
  the later pages unvisited, so `purge_shard` could report a tenant erased with objects left.

  This parses with OTP's `:xmerl_sax_parser` — in OTP, no new dependency. SAX rather than
  `:xmerl_scan` on purpose: the DOM scanner interns every element name as an atom, and this input
  is a network response. SAX hands back strings and returns errors instead of exiting.

  A DTD is refused outright (a ListObjectsV2 response never has one; entity expansion is the one
  way a small body becomes a large one), and the parser is aborted on the first one.

  `parse/1` returns an error rather than a short page when the response says `IsTruncated=true`
  but carries no `NextContinuationToken`: continuing is impossible and stopping would silently
  report a partial listing as complete.
  """

  @type entry :: %{key: String.t(), size: non_neg_integer() | nil}
  @type t :: %{entries: [entry()], next: String.t() | nil}

  @spec parse(term()) :: {:ok, t()} | {:error, term()}
  def parse(xml) when is_binary(xml) do
    init = %{path: [], text: [], contents: nil, entries: [], truncated: false, token: nil}

    # The SAX parser catches a throw from the event fun and hands it back as a 5-tuple headed by
    # the thrown tag — that is how `:list_page_abort` below arrives.
    case :xmerl_sax_parser.stream(xml, event_fun: &event/3, event_state: init) do
      {:list_page_abort, _loc, reason, _tags, _state} -> {:error, reason}
      {:ok, state, _rest} -> finish(state)
      {:fatal_error, _loc, reason, _tags, _state} -> {:error, {:list_xml, to_string_safe(reason)}}
      other -> {:error, {:list_xml, inspect(other)}}
    end
  end

  def parse(other), do: {:error, {:list_body_not_xml, inspect(other, limit: 5)}}

  defp finish(%{truncated: true, token: nil}), do: {:error, :list_truncated_without_token}

  defp finish(state),
    do: {:ok, %{entries: Enum.reverse(state.entries), next: state.token}}

  defp to_string_safe(reason) do
    List.to_string(reason)
  rescue
    _ -> inspect(reason)
  end

  defp event({:startDTD, _name, _public, _system}, _loc, _state),
    do: throw({:list_page_abort, :list_xml_dtd_refused})

  defp event({:startElement, _uri, local, _qname, _attrs}, _loc, state) do
    name = List.to_string(local)
    contents = if name == "Contents", do: %{key: nil, size: nil}, else: state.contents
    %{state | path: [name | state.path], text: [], contents: contents}
  end

  defp event({:characters, chars}, _loc, %{path: [_ | _]} = state),
    do: %{state | text: [chars | state.text]}

  defp event({:endElement, _uri, local, _qname}, _loc, state) do
    name = List.to_string(local)
    text = state.text |> Enum.reverse() |> List.to_string()
    [^name | parents] = state.path
    state = %{state | path: parents, text: []}

    case {name, parents} do
      {"IsTruncated", ["ListBucketResult"]} ->
        %{state | truncated: String.trim(text) == "true"}

      {"NextContinuationToken", ["ListBucketResult"]} ->
        %{state | token: if(text == "", do: nil, else: text)}

      {"Key", ["Contents", "ListBucketResult"]} ->
        %{state | contents: %{state.contents | key: text}}

      {"Size", ["Contents", "ListBucketResult"]} ->
        size =
          case Integer.parse(String.trim(text)) do
            {n, ""} when n >= 0 -> n
            _ -> nil
          end

        %{state | contents: %{state.contents | size: size}}

      {"Contents", ["ListBucketResult"]} ->
        case state.contents do
          %{key: key} = entry when is_binary(key) ->
            %{state | contents: nil, entries: [entry | state.entries]}

          _ ->
            throw({:list_page_abort, :list_contents_without_key})
        end

      _ ->
        state
    end
  end

  defp event(_other, _loc, state), do: state
end
