defmodule Fathom.SqlLead do
  @moduledoc """
  The FRONT of a SQL statement as SQLite's parser will actually run it.

  Every head classifier in fathom — the tenant executor's authorization gates and the migration
  capture's data-migration lint — must agree on where a statement starts, or a prefix trick seen
  through by one is missed by the other. That is the defect class of expert review 2026-08-20 #19,
  2026-08-24 #1 and 2026-09-05 #1 in the executor, and of 2026-09-29 #18 in capture, which kept its
  own `String.trim_leading/1` and so read `-- backfill\\nUPDATE …` as not-DML. One function, used by
  both, is the fix for the class rather than for the instance.

  Not a SQL parser: SQLite exposes none through exqlite (its `prepare/2` discards the tail), and the
  only question asked here is "what is the first token". Kept small and in one place.
  """

  @doc """
  Strips, in a loop, insignificant leading whitespace, a UTF-8 BOM, both SQL comment forms, and
  empty statements (a bare `;` — `ecmd ::= SEMI` in SQLite's grammar, so `;PRAGMA …` parses and RUNS
  the pragma). An unterminated comment leaves no statement behind and returns `""`, which every
  caller treats as the conservative case.
  """
  @spec strip(String.t()) :: String.t()
  def strip(sql) do
    case String.trim_leading(sql) do
      # A UTF-8 BOM (U+FEFF): SQLite's tokenizer skips a leading one and RUNS the statement, but
      # `String.trim_leading/1` does NOT — U+FEFF is Unicode Cf (format), not White_Space
      # (verified by execution 2026-09-13).
      "﻿" <> rest -> strip(rest)
      "/*" <> rest -> rest |> after_delim("*/") |> strip()
      "--" <> rest -> rest |> after_delim("\n") |> strip()
      ";" <> rest -> strip(rest)
      other -> other
    end
  end

  defp after_delim(bin, delim) do
    case :binary.match(bin, delim) do
      {i, len} -> binary_part(bin, i + len, byte_size(bin) - i - len)
      :nomatch -> ""
    end
  end
end
