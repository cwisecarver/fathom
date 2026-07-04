defmodule Fathom.Shard.Storage.S3CopyErrorTest do
  # Round-2 expert review #8: S3 CopyObject can return 200 OK and then stream an
  # `<Error>` XML body when the copy fails AFTER response headers are sent (long
  # server-side copies). copy_object/touch_object matched on status alone, so a
  # failed-but-"200" copy was read as success — a silent no-op that (for touch_object)
  # evaporates the steal fence and (for retain) leaves the migration revert backup
  # nonexistent. A real success carries `<CopyObjectResult>`; an `<Error>` body must be
  # a failure.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  @error_body ~s(<?xml version="1.0"?><Error><Code>InternalError</Code><Message>boom</Message></Error>)
  @ok_body ~s(<?xml version="1.0"?><CopyObjectResult><ETag>"abc"</ETag></CopyObjectResult>)

  defp put_s3_config(plug) do
    prev = Application.get_env(:fathom, S3)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "https://s3.example",
      path_style: true,
      req_plug: plug
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)
  end

  test "a copy that returns 200 with an <Error> body is a failure, not success" do
    put_s3_config(fn conn -> Plug.Conn.send_resp(conn, 200, @error_body) end)

    # retain/2 goes through copy_object; pre-fix this silently returned :ok.
    assert {:error, {:s3_copy_error_body, _}} = S3.retain("shard_x", 1)
  end

  test "a copy that returns 200 with a <CopyObjectResult> body succeeds" do
    put_s3_config(fn conn -> Plug.Conn.send_resp(conn, 200, @ok_body) end)
    assert :ok = S3.retain("shard_x", 1)
  end

  test "an empty 2xx body (S3-compatible stores) is still a success" do
    put_s3_config(fn conn -> Plug.Conn.send_resp(conn, 200, "") end)
    assert :ok = S3.retain("shard_x", 1)
  end
end
