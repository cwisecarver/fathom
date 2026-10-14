defmodule Fathom.Shard.Storage.S3.UploadsPage do
  @moduledoc """
  One parsed ListMultipartUploads response page (expert review 2026-10-13 #21).

  `purge_shard` lists objects only, so an in-progress multipart upload (a large-object steal-touch
  that was killed between create and complete) holds server-side copies of tenant data that
  erasure never sees. This parses the listing that finds them, the same way `ListPage` parses
  ListObjectsV2: OTP's `:xmerl_sax_parser` (strings, not interned atoms; errors, not exits), a DTD
  refused outright, and an error rather than a short page when the response says it is truncated
  but gives no marker to continue with.
  """

  @type upload :: %{key: String.t(), upload_id: String.t()}
  @type t :: %{uploads: [upload()], next: {String.t(), String.t()} | nil}

  @spec parse(term()) :: {:ok, t()} | {:error, term()}
  def parse(xml) when is_binary(xml) do
    init = %{
      path: [],
      text: [],
      upload: nil,
      uploads: [],
      truncated: false,
      key_marker: nil,
      id_marker: nil
    }

    case :xmerl_sax_parser.stream(xml, event_fun: &event/3, event_state: init) do
      {:uploads_page_abort, _loc, reason, _tags, _state} -> {:error, reason}
      {:ok, state, _rest} -> finish(state)
      {:fatal_error, _loc, reason, _tags, _state} -> {:error, {:uploads_xml, safe(reason)}}
      other -> {:error, {:uploads_xml, inspect(other)}}
    end
  end

  def parse(other), do: {:error, {:uploads_body_not_xml, inspect(other, limit: 5)}}

  defp finish(%{truncated: true, key_marker: k, id_marker: i})
       when k in [nil, ""] or i in [nil, ""],
       do: {:error, :uploads_truncated_without_marker}

  defp finish(%{truncated: true} = state),
    do: {:ok, %{uploads: Enum.reverse(state.uploads), next: {state.key_marker, state.id_marker}}}

  defp finish(state), do: {:ok, %{uploads: Enum.reverse(state.uploads), next: nil}}

  defp safe(reason) do
    List.to_string(reason)
  rescue
    _ -> inspect(reason)
  end

  defp event({:startDTD, _name, _public, _system}, _loc, _state),
    do: throw({:uploads_page_abort, :uploads_xml_dtd_refused})

  defp event({:startElement, _uri, local, _qname, _attrs}, _loc, state) do
    name = List.to_string(local)
    upload = if name == "Upload", do: %{key: nil, upload_id: nil}, else: state.upload
    %{state | path: [name | state.path], text: [], upload: upload}
  end

  defp event({:characters, chars}, _loc, %{path: [_ | _]} = state),
    do: %{state | text: [chars | state.text]}

  defp event({:endElement, _uri, local, _qname}, _loc, state) do
    name = List.to_string(local)
    text = state.text |> Enum.reverse() |> List.to_string()
    [^name | parents] = state.path
    state = %{state | path: parents, text: []}

    case {name, parents} do
      {"IsTruncated", ["ListMultipartUploadsResult"]} ->
        %{state | truncated: String.trim(text) == "true"}

      {"NextKeyMarker", ["ListMultipartUploadsResult"]} ->
        %{state | key_marker: text}

      {"NextUploadIdMarker", ["ListMultipartUploadsResult"]} ->
        %{state | id_marker: text}

      {"Key", ["Upload", "ListMultipartUploadsResult"]} ->
        %{state | upload: %{state.upload | key: text}}

      {"UploadId", ["Upload", "ListMultipartUploadsResult"]} ->
        %{state | upload: %{state.upload | upload_id: text}}

      {"Upload", ["ListMultipartUploadsResult"]} ->
        case state.upload do
          %{key: k, upload_id: i} = up when is_binary(k) and is_binary(i) ->
            %{state | upload: nil, uploads: [up | state.uploads]}

          _ ->
            throw({:uploads_page_abort, :upload_without_key_or_id})
        end

      _ ->
        state
    end
  end

  defp event(_other, _loc, state), do: state
end
