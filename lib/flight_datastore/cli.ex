defmodule FlightDatastore.CLI do
  def main(arguments) do
    {_opts, args, _} = OptionParser.parse(arguments)

    data = parse_data("FLIGHT_DATA")
    credential = parse_data("FLIGHT_CREDENTIAL")

    case args do
      ["find",         opts | _] -> opts |> parse_json |> find(data,credential)
      ["query",        opts | _] -> opts |> parse_json |> query(data,credential)
      ["modify",       opts | _] -> opts |> parse_json |> modify(data,credential)
      ["upload",       opts | _] -> opts |> parse_json |> upload(data,credential)
      ["bulk-insert",  opts | _] -> opts |> parse_json |> bulk_insert(data,credential)
      ["purge-upload", opts | _] -> opts |> parse_json |> purge_upload(data,credential)

      _ -> "unknown command: #{arguments |> inspect}" |> puts_error
    end
  end

  defp find(opts,data,_credential) do
    case FlightDatastore.find(%{
      namespace: data["namespace"],
      kind: data["kind"],
      key: data["key"],
      conditions: data["conditions"],
      columns: data["columns"],
      scope: opts["scope"],
    }) do
      {:error, message} -> message     |> puts_result(105)
      {:ok, nil}        -> "not found" |> puts_result(104)
      {:ok, entity} -> entity |> puts_result
    end
  end

  defp query(opts,data,_credential) do
    case FlightDatastore.query(%{
      namespace: data["namespace"],
      kind: data["kind"],
      conditions: data["conditions"],
      columns: data["columns"],
      order_column: data["order_column"],
      order: data["order"],
      limit: data["limit"],
      offset: data["offset"],
      scope: opts["scope"],
    }) do
      {:error, :not_allowed} -> "not allowed" |> puts_result(105)
      {:error, :not_allowed, message} -> "not allowed: #{message}" |> puts_result(105)
      {:error, :execute_failed, message} -> "execute failed: #{message}" |> puts_result(100)
      {:ok, result} -> result |> puts_result
    end
  end

  defp modify(opts,data,credential) do
    case FlightDatastore.modify(%{
      data: data,
      scope: opts["scope"],
      credential: credential,
    }) do
      {:ok, result} -> result |> puts_result
      {:error, :not_allowed} -> "not allowed" |> puts_result(105)
      {:error, :bad_request, message} -> "bad request: #{message}" |> puts_result(100)
      {:error, :not_found,   message} -> "not found: #{message}"   |> puts_result(104)
      {:error, :conflict,    message} -> "conflict: #{message}"    |> puts_result(109)
    end
  end

  defp upload(opts,data,credential) do
    case FlightDatastore.modify(%{
      data: data
        |> Enum.map(fn info ->
          %{
            "action" => "insert",
            "namespace" => info["namespace"],
            "kind" => info["kind"],
            "key" => info["key"],
            "properties" => info,
          }
        end),
      scope: opts["scope"],
      credential: credential,
    }) do
      {:ok, result} ->
        result.data
        |> Enum.map(fn entity -> entity["properties"] end)
        |> puts_result
      {:error, :not_allowed} -> "not allowed" |> puts_result(105)
      {:error, :bad_request, message} -> "bad request: #{message}" |> puts_result(100)
      {:error, :not_found,   message} -> "not found: #{message}"   |> puts_result(104)
      {:error, :conflict,    message} -> "conflict: #{message}"    |> puts_result(109)
    end
  end

  defp bulk_insert(opts,data,credential) do
    data
    |> Enum.map(fn info ->
      case FlightDatastore.bulk_insert(%{
        data: info,
        src: opts["src"],
        dest: opts["dest"],
        action: opts["action"] || "insert",
        data_kind: opts["data"],
        scope: opts["scope"],
        credential: credential
      }) do
        {:ok, result} -> result
        {:error, message} -> message |> puts_error
      end
    end)
    |> puts_result
  end

  defp purge_upload(opts,data,credential) do
    case FlightDatastore.purge_upload(%{
      data: data,
      data_kinds: opts["data"],
      scope: opts["scope"],
      credential: credential
    }) do
      {:ok, result} -> result |> puts_result
      {:error, :not_allowed} -> "not allowed" |> puts_result(105)
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
