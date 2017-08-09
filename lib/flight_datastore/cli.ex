defmodule FlightDatastore.CLI do
  def main(arguments) do
    data = System.get_env("FLIGHT_DATA") |> Poison.decode!

    {_opts, args, _} = OptionParser.parse(arguments)
    case args do
      ["find", kind | _] ->
        kind
        |> FlightDatastore.find(data["key"],data["conditions"],data["columns"])
        |> puts_result

      _ -> "unknown command: #{arguments |> inspect}" |> puts_error
    end
  end

  defp puts_result(data) do
    IO.puts(data |> Poison.encode!)
  end
  defp puts_error(message) do
    IO.puts(:stderr, "#{__MODULE__}: [ERROR] #{message}")
  end
end
