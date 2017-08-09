defmodule FlightDatastore.CLI do
  def main(args) do
    {opts, args, _} = OptionParser.parse(args, strict: [file: :string])
    case args do
      ["find", kind | _] ->
        case opts[:file] do
          nil -> puts_error("data file no detected")
          file ->
            case File.read(file) do
              {:error, message} -> puts_error(message)
              {:ok, data} ->
                case Poison.decode(data) do
                  {:error, message} -> puts_error(inspect(message))
                  {:error, message, data} -> puts_error(inspect({message, data}))
                  {:ok, json} ->
                    json = FlightDatastore.find(kind,json["key"],json["conditions"],json["columns"])
                           |> Poison.encode!
                    case File.write(file, json) do
                      :ok -> nil
                      {:error, message} -> puts_error(message)
                    end
                end
            end
        end

      _ -> puts_error("unknown command")
    end
  end

  defp puts_error(message) do
    IO.puts(:stderr, "flight_datastore: [ERROR] #{message}")
  end
end
