defmodule FlightDatastore.BulkInsert do
  @moduledoc """
  Bulk insert entity utils
  """

  alias FlightDatastore.Modify

  @error_kind "_Flight_BulkInsertError"
  @key_column "_bulk_insert_key"
  @file_column "_bulk_insert_file"
  @bulk_unit 300

  def insert_data(info,scope) do
    case open_output(info.dest,info.data["name"]) do
      {:ok,out} ->
        case open_input(info.src,info.data["name"]) do
          {:ok, file} ->
            result = file
            |> IO.stream(:line)
            |> Enum.chunk_every(@bulk_unit)
            |> Enum.reduce(true, fn lines, acc ->
              data = lines |> parse(info,scope)
              case data |> insert do
                {:ok, _response} ->
                  data |> Enum.each(fn info ->
                    out |> IO.puts(info["properties"] |> Poison.encode!)
                  end)
                  acc and true
                {:error, status} ->
                  insert_error(data,status,info.data)
                  false
              end
            end)

            file |> File.close
            out  |> File.close
            {:ok, result}

          error -> error
        end
      error -> error
    end
  end

  defp open_input(src,file) do
    [src,file] |> Path.join |> open_file
  end
  defp open_output(dest,file) do
    path = [dest,file] |> Path.join
    case File.mkdir_p(path |> Path.dirname) do
      {:error, message} -> {:error, "failed mkdir_p: #{message} [#{path}]"}
      _ -> path |> open_file([:write])
    end
  end
  defp open_file(path,mode \\ []) do
    case path |> File.open([:utf8 | mode]) do
      {:error, message} -> {:error, "failed open file: #{message} [#{path}]"}
      result -> result
    end
  end


  @doc """
  Generate key

  ## Examples

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column1","column2"], %{"namespace" => "Namespace", "kind" => "File", "name" => "file.txt"})
      "Namespace:File:file.txt:value1:value2"

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, ["column_unknown"], %{"namespace" => "Namespace", "kind" => "File", "name" => "file.txt"})
      "Namespace:File:file.txt:"

      iex> FlightDatastore.BulkInsert.generate_key(%{"column1" => "value1", "column2" => "value2", "column3" => "value3"}, [], %{"kind" => "File", "name" => "file.txt"})
      ":File:file.txt"
  """
  def generate_key(data, keys, info) do
    [info |> file_signature | keys |> Enum.map(fn key -> data[key] end)] |> Enum.join(":")
  end

  defp parse(lines,info,scope) do
    lines
    |> Enum.map(fn line ->
      props = line |> Poison.decode! |> fill(scope,info.data)
      %{
        "action" => info.action,
        "namespace" => info.data["namespace"],
        "kind" => info.data[info.data_kind],
        "key" => props[@key_column],
        "properties" => props,
      }
    end)
  end
  defp insert(data) do
    data
    |> Modify.to_request
    |> Diplomat.Entity.commit_request
    |> Diplomat.Client.commit
  end
  defp fill(data, scope, info) do
    data
    |> Map.put(@key_column, generate_key(data, scope["keys"], info))
    |> Map.put(@file_column, info |> file_signature)
  end

  defp insert_error(data,status,info) do
    [%{
      "kind" => info |> error_kind,
      "action" => "insert",
      "properties" => %{
        "data" => data,
        "message" => status.message,
      },
    }]
    |> Modify.execute
  end


  def save_result(info,result,message) do
    [%{
      "namespace" => info.data["namespace"],
      "kind" => info.data["kind"],
      "key" => info.data["key"],
      "action" => "update",
      "properties" => %{
        "bulk_insert_#{info.data_kind}" => %{
          result: result,
          message: message,
          error: info.data |> error_kind,
        },
      },
    }]
    |> Modify.execute

    info |> Modify.log(info.credential)

    info.data
  end

  def file_column do
    @file_column
  end
  def error_kind(info) do
    "#{@error_kind}:#{info |> file_signature}"
  end
  def file_signature(info) do
    "#{info["namespace"]}:#{info["kind"]}:#{info["name"]}"
  end
end
