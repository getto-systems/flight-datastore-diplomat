defmodule FlightDatastore.CLI do
  def main(arguments) do
    {_opts, args, _} = OptionParser.parse(arguments)

    data = parse_data("FLIGHT_DATA")
    credential = parse_data("FLIGHT_CREDENTIAL")

    case args do
      ["format-for-upload", opts | _] -> opts |> parse_json |> format_for_upload(data,credential)
      ["find",              opts | _] -> opts |> parse_json |> find(data,credential)
      ["modify",            opts | _] -> opts |> parse_json |> modify(data,credential)

      _ -> "unknown command: #{arguments |> inspect}" |> puts_error
    end
  end

  defp format_for_upload(opts,data,_credential) do
    kind = opts["kind"]
    path = opts["path"]

    data
    |> Enum.map(fn info ->
      %{
        kind: kind,
        action: :insert,
        key: info["name"],
        properties: info |> Map.put("path", [path,info["name"]] |> Path.join),
      }
    end)
    |> puts_result
  end

  defp find(opts,data,_credential) do
    kind = opts["kind"]
    scope = opts["scope"]

    kind
    |> FlightDatastore.find(
      data["key"],
      data["conditions"],
      data["columns"],
      scope
    )
    |> case do
      nil -> "not found" |> puts_result(104)
      entity -> entity |> puts_result
    end
  end

  defp modify(opts,data,credential) do
    scope = opts["scope"]
    data
    |> FlightDatastore.modify(
      scope,
      credential
    )
    |> case do
      {:ok, result} -> result |> puts_result
      {:error, :not_allowed} -> "not allowed" |> puts_result(105)
      {:error, :bad_request, message} -> "bad request: #{message}" |> puts_result(100)
      {:error, :not_found,   message} -> "not found: #{message}"   |> puts_result(104)
      {:error, :conflict,    message} -> "conflict: #{message}"    |> puts_result(109)
    end
  end

  defp parse_data(key) do
    System.get_env(key)
    |> parse_json
  end
  defp parse_json(json) do
    json
    |> case do
      nil -> %{}
      raw ->
        raw
        |> Poison.decode!
        |> case do
          nil -> %{}
          data -> data
        end
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
