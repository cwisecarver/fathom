defmodule Fathom.RuntimeAdminConfigTest do
  @moduledoc """
  Expert review 2026-10-13 #9: `config/runtime.exs` reads `ADMIN_USER`/`ADMIN_PASS` and must treat a
  blank value as UNSET. Before the fix, `ADMIN_USER=` configured `username: ""`, and the router
  accepted `":"` against it. This evaluates the real runtime config file in dev, so the blank
  normalisation is tested where it happens, not against a copy of it.

  Not async: it sets process-global OS environment variables.
  """
  use ExUnit.Case, async: false

  @vars ["ADMIN_USER", "ADMIN_PASS", "ADMIN_WEAK_CREDENTIALS_OK"]

  setup do
    prev = Map.new(@vars, &{&1, System.get_env(&1)})

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v), do: System.delete_env(k), else: System.put_env(k, v)
      end
    end)

    Enum.each(@vars, &System.delete_env/1)
    :ok
  end

  defp runtime_config(env_vars) do
    Enum.each(env_vars, fn {k, v} -> System.put_env(k, v) end)
    Config.Reader.read!(Path.expand("config/runtime.exs", File.cwd!()), env: :dev, target: nil)
  end

  defp admin_auth(config), do: config |> Keyword.get(:fathom, []) |> Keyword.get(:admin_auth)

  test "blank ADMIN_USER and ADMIN_PASS leave :admin_auth unset and flag the blank env" do
    config = runtime_config(%{"ADMIN_USER" => "", "ADMIN_PASS" => ""})

    assert admin_auth(config) == nil
    assert Keyword.get(config[:fathom], :admin_auth_blank_env) == true
  end

  test "whitespace-only values are blank too" do
    config = runtime_config(%{"ADMIN_USER" => "admin", "ADMIN_PASS" => "   "})

    assert admin_auth(config) == nil
    assert Keyword.get(config[:fathom], :admin_auth_blank_env) == true
  end

  test "a real pair is configured and is not flagged as blank" do
    config = runtime_config(%{"ADMIN_USER" => "admin", "ADMIN_PASS" => "0123456789abcdef"})

    assert admin_auth(config) == [username: "admin", password: "0123456789abcdef"]
    assert Keyword.get(config[:fathom], :admin_auth_blank_env) == nil
  end

  test "ADMIN_WEAK_CREDENTIALS_OK=true sets the opt-out; anything else does not" do
    on = runtime_config(%{"ADMIN_WEAK_CREDENTIALS_OK" => "true"})
    assert Keyword.get(on[:fathom], :admin_weak_credentials_ok) == true

    off = runtime_config(%{"ADMIN_WEAK_CREDENTIALS_OK" => "yes"})
    assert Keyword.get(off[:fathom], :admin_weak_credentials_ok) == nil
  end
end
