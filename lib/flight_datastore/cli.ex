defmodule FlightDatastore.CLI do
  def main(arguments) do
    {_opts, args, _} = OptionParser.parse(arguments)

    data = parse_data("FLIGHT_DATA")
    credential = parse_data("FLIGHT_CREDENTIAL")

    case args do
      ["find",        opts | _] -> opts |> parse_json |> find(data,credential)
      ["modify",      opts | _] -> opts |> parse_json |> modify(data,credential)
      ["upload",      opts | _] -> opts |> parse_json |> upload(data,credential)
      ["bulk-insert", opts | _] -> opts |> parse_json |> bulk_insert(data,credential)

      _ -> "unknown command: #{arguments |> inspect}" |> puts_error
    end
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
    |> FlightDatastore.modify(scope,credential)
    |> case do
      {:ok, result} -> result |> puts_result
      {:error, :not_allowed} -> "not allowed" |> puts_result(105)
      {:error, :bad_request, message} -> "bad request: #{message}" |> puts_result(100)
      {:error, :not_found,   message} -> "not found: #{message}"   |> puts_result(104)
      {:error, :conflict,    message} -> "conflict: #{message}"    |> puts_result(109)
    end
  end

  defp upload(_opts,data,credential) do
    kind = data |> Enum.reduce(nil,fn info,_acc -> info["kind"] end)
    cols = data |> Enum.reduce([],fn info,_acc -> info |> Map.keys end)

    data
    |> Enum.map(fn info ->
      %{
        "kind" => info["kind"],
        "action" => "insert",
        "key" => info["name"],
        "properties" => info,
      }
    end)
    |> FlightDatastore.modify(%{
      kind => %{
        "insert" => %{
          "cols" => cols,
        },
      },
    },credential)
    |> case do
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

  defp bulk_insert(opts,data,_credential) do
    src = opts["src"]
    dest = opts["dest"]
    kind = opts["kind"]
    keys = opts["keys"]
    fill = opts["fill"]

    data
    |> Enum.map(fn info ->
      case open_output(dest,info["name"]) do
        {:ok,out} ->
          [src,info["name"]]
          |> Path.join
          |> File.open([:utf8])
          |> case do
            {:ok, file} ->
              result = file
              |> IO.stream(:line)
              |> Enum.reduce(true, fn line, acc ->
                line
                |> parse_json
                |> FlightDatastore.bulk_insert(kind,keys,fill,info,out)
                |> case do
                  :ok -> acc and true
                  :error -> false
                end
              end)

              file |> File.close
              out  |> File.close
              {:ok, result}

            error -> error
          end
        error -> error
      end
      |> case do
        {:ok, result} ->
          if result do
            info
          else
            info |> Map.put(:bulk_insert_error, info |> FlightDatastore.BulkInsert.error_kind)
          end
        {:error, message} -> message |> puts_error
      end
    end)
    |> puts_result
  end
  defp open_output(output,file) do
    path = [output,file] |> Path.join
    case File.mkdir_p(path |> Path.dirname) do
      {:error, message} -> {:error, "failed mkdir_p: #{message} [#{path}]"}
      _ ->
        path
        |> File.open([:write, :utf8])
        |> case do
          {:error, message} -> {:error, "failed open file: #{message} [#{path}]"}
          result -> result
        end
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
