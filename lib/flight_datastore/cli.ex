defmodule FlightDatastore.CLI do
  def main(arguments) do
    data = parse_data("FLIGHT_DATA")
    credential = parse_data("FLIGHT_CREDENTIAL")

    {_opts, args, _} = OptionParser.parse(arguments)
    case args do
      ["find", kind | _] ->
        kind
        |> FlightDatastore.find(data["key"],data["conditions"],data["columns"])
        |> case do
          nil -> "not found" |> puts_result(104)
          entity -> entity |> puts_result
        end

      ["modify" | kinds] ->
        data
        |> FlightDatastore.modify(kinds,credential)
        |> case do
          {:ok, result} -> result |> puts_result
          {:error, :not_allowed} -> "not allowed" |> puts_result(105)
          {:error, :bad_request, message} -> "bad request: #{message}" |> puts_result(100)
          {:error, :not_found,   message} -> "not found: #{message}"   |> puts_result(104)
          {:error, :conflict,    message} -> "conflict: #{message}"    |> puts_result(109)
        end

      _ -> "unknown command: #{arguments |> inspect}" |> puts_error
    end
  end

  defp parse_data(key) do
    System.get_env(key)
    |> Poison.decode!
    |> case do
      nil -> %{}
      data -> data
    end
  end

  defp puts_result(data) do
    IO.puts(data |> Poison.encode!)
  end
  defp puts_result(message,status) do
    IO.puts(message)
    System.halt(status)
  end
  defp puts_error(message) do
    IO.puts(:stderr, "#{__MODULE__}: [ERROR] #{message}")
    System.halt(1)
  end
end
