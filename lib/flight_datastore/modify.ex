defmodule FlightDatastore.Modify do
  @moduledoc """
  Modify entity utils
  """

  @update_kind "_Flight_Update"
  @log_kind "_Flight_Log"

  alias FlightDatastore.Find

  @doc """
  Convert kind scope list to kind scope map

  ## Examples

      iex> FlightDatastore.Modify.to_scope_map(["User:insert", "Profile:update:nolog"])
      %{"User" => %{"insert" => %{log: true}}, "Profile" => %{"update" => %{log: false}}}

      iex> FlightDatastore.Modify.to_scope_map(["User:insert", "User:replace", "User:replace:nolog"])
      %{"User" => %{"insert" => %{log: true}, "replace" => %{log: false}}}
  """
  def to_scope_map(kinds) do
    defaults = %{
      log: true,
    }

    kinds
    |> Enum.reduce(%{}, fn kind_scope, acc ->
      case kind_scope |> String.split(":") do
        [kind, action | scopes] ->
          scope = acc |> Map.get(kind, %{})
          action_scope = scope |> Map.get(action, defaults) |> Map.merge(scopes |> parse_scopes)
          acc |> Map.put(kind, scope |> Map.put(action, action_scope))
        _ -> acc
      end
    end)
  end
  defp parse_scopes(scopes) do
    scopes |> Enum.reduce(%{}, fn term, acc ->
      case term do
        "nolog" -> acc |> Map.put(:log, false)
        _ -> acc
      end
    end)
  end

  @doc """
  Check permission to modify data

  ## Examples

      iex> FlightDatastore.Modify.check([%{"kind" => "User", "action" => "insert"}], %{"User" => %{"insert" => %{}}})
      true

      iex> FlightDatastore.Modify.check([%{"kind" => "User", "action" => "update"}], %{"User" => %{"insert" => %{}}})
      false

      iex> FlightDatastore.Modify.check([%{"kind" => "Profile", "action" => "update"}], %{"User" => %{"insert" => %{}}})
      false

      iex> FlightDatastore.Modify.check([], %{"User" => %{"insert" => %{}}})
      false

      iex> FlightDatastore.Modify.check(nil, %{"User" => %{"insert" => %{}}})
      false
  """
  def check(nil,_scopes), do: false
  def check([],_scopes), do: false
  def check(data,scopes) do
    data |> Enum.all?(fn info -> scopes[info["kind"]][info["action"]] end)
  end

  @doc """
  Execute modify
  """
  def execute(data) do
    data
    |> to_request
    |> commit
  end

  @doc """
  Convert operates to list of commit request

  ## Examples

      iex> FlightDatastore.Modify.to_request([%{"action" => "insert", "kind" => "User", "properties" => %{"name" => "user name", "email" => "user@example.com"}}, %{"action" => "update", "kind" => "Summary", "key" => 1, "properties" => %{"user_count" => 1}}, %{"action" => "delete", "kind" => "Guest", "key" => "guest"}])
      [{:insert,%Diplomat.Entity{key: %Diplomat.Key{id: nil, kind: "User", name: nil, namespace: nil, parent: nil, project_id: nil}, kind: "User", properties: %{"name" => %Diplomat.Value{value: "user name"}, "email" => %Diplomat.Value{value: "user@example.com"}}}}, {:update,%Diplomat.Entity{key: %Diplomat.Key{id: 1, kind: "Summary", name: nil, namespace: nil, parent: nil, project_id: nil}, kind: "Summary", properties: %{"user_count" => %Diplomat.Value{value: 1}}}}, {:delete,%Diplomat.Key{id: nil, kind: "Guest", name: "guest", namespace: nil, parent: nil, project_id: nil}}]

      iex> FlightDatastore.Modify.to_request([%{"action" => "replace", "kind" => "User", "key" => "user", "old-key" => "old-user", "properties" => %{"name" => "user name"}}])
      [{:update,%Diplomat.Entity{key: %Diplomat.Key{id: nil, kind: "User", name: "old-user", namespace: nil, parent: nil, project_id: nil}, kind: "User", properties: %{"name" => %Diplomat.Value{value: "user name"}}}}, {:delete,%Diplomat.Key{id: nil, kind: "User", name: "old-user", namespace: nil, parent: nil, project_id: nil}}, {:insert,%Diplomat.Entity{key: %Diplomat.Key{id: nil, kind: "User", name: "user", namespace: nil, parent: nil, project_id: nil}, kind: "User", properties: %{"name" => %Diplomat.Value{value: "user name"}}}}]
  """
  def to_request(data) do
    data
    |> Enum.flat_map(fn info ->
      case info["action"] do
        "delete" = action ->
          [{
            :"#{action}",
            Diplomat.Key.new(info["kind"],info["key"]),
          }]
        "replace" ->
          [{
            :update,
            info["properties"] |> Diplomat.Entity.new(info["kind"],info["old-key"]),
          },{
            :delete,
            Diplomat.Key.new(info["kind"],info["old-key"]),
          },{
            :insert,
            info["properties"] |> Diplomat.Entity.new(info["kind"],info["key"]),
          }]
        action ->
          [{
            :"#{action}",
            info["properties"] |> Diplomat.Entity.new(info["kind"],info["key"]),
          }]
      end
    end)
  end

  def commit(request) do
    request
    |> Diplomat.Entity.commit_request(:TRANSACTIONAL,Diplomat.Transaction.begin)
    |> Diplomat.Client.commit
  end

  @doc """
  Get inserted keys from response

  ## Examples

      iex> FlightDatastore.Modify.inserted_keys(%Diplomat.Proto.CommitResponse{ index_updates: 6, mutation_results: [ %Diplomat.Proto.MutationResult{ key: %Diplomat.Proto.Key{ partition_id: %Diplomat.Proto.PartitionId{namespace_id: nil, project_id: "neon-circle-164919"}, path: [%Diplomat.Proto.Key.PathElement{id_type: {:id, 5730082031140864}, kind: "User"}] } }, %Diplomat.Proto.MutationResult{ key: %Diplomat.Proto.Key{ partition_id: %Diplomat.Proto.PartitionId{namespace_id: nil, project_id: "neon-circle-164919"}, path: [%Diplomat.Proto.Key.PathElement{id_type: {:id, 5167132077719552}, kind: "User"}] } } ] })
      [5730082031140864,5167132077719552]
  """
  def inserted_keys(response) do
    response.mutation_results |> Enum.flat_map(fn result ->
      case result.key do
        nil -> []
        key -> key.path |> Enum.flat_map(fn path ->
          case path.id_type do
            {:id, id} -> [id]
            _ -> []
          end
        end)
      end
    end)
  end

  @doc """
  Fill inserted keys to request data

  ## Examples

      iex> FlightDatastore.Modify.fill_keys([%{"action" => "insert"}],[1])
      [%{"action" => "insert", "key" => 1}]

      iex> FlightDatastore.Modify.fill_keys([%{"action" => "insert"}, %{"action" => "insert"}],[1,2])
      [%{"action" => "insert", "key" => 1}, %{"action" => "insert", "key" => 2}]

      iex> FlightDatastore.Modify.fill_keys([%{"action" => "insert", "key" => "key"}, %{"action" => "insert"}],[1])
      [%{"action" => "insert", "key" => "key"}, %{"action" => "insert", "key" => 1}]

      iex> FlightDatastore.Modify.fill_keys([%{"action" => "update"},%{"action" => "delete"},%{"action" => "insert"}],[1])
      [%{"action" => "update"},%{"action" => "delete"},%{"action" => "insert", "key" => 1}]

      iex> FlightDatastore.Modify.fill_keys([%{"action" => "update"}],[])
      [%{"action" => "update"}]
  """
  def fill_keys(data,keys) do
    keys |> Enum.reduce(data, fn key, acc -> key |> fill_key(acc) end)
  end
  defp fill_key(key,data) do
    finder = fn info ->
      info["action"] == "insert" && info["key"] == nil
    end
    case data |> Enum.find_index(finder) do
      nil -> data
      index ->
        {first, last} = data |> Enum.split(index)
        [target | tail] = last
        filled = target |> Map.put("key", key)
        first ++ [filled] ++ tail
    end
  end

  @doc """
  Logging updates
  """
  def log(data,scopes,operator) do
    data
    |> Enum.each(fn info ->
      if scopes[info["kind"]][info["action"]][:log] do
        info |> rec(operator)
      end
    end)
  end
  defp rec(info,operator) do
    update = %{
      "kind" => info["kind"],
      "key" => info["key"],
      "at" => DateTime.utc_now |> DateTime.to_iso8601,
      "salt" => :rand.uniform(),
      "operator" => operator,
    }

    log = info
          |> Map.put("at", update["at"])
          |> Map.put("salt", update["salt"])
          |> Map.put("operator", update["operator"])

    case info["action"] do
      "insert" ->
        [
          {:insert, log |> to_log},
          {:insert, update |> to_update("first")},
          {:upsert, update |> to_update("last")},
        ] |> commit
      _ ->
        last = info |> find_last

        request = [
          {:insert, log |> Map.put("last", last |> to_log_key) |> to_log},
          {:upsert, update |> to_update("last")},
        ]

        case last |> find_log do
          nil -> request
          last_log -> [{:update, last_log |> Map.put("next", log |> to_log_key) |> to_log} | request]
        end |> commit
    end
  end

  defp find_last(data) do
    Find.find_entity(@update_kind, data |> to_update_key("last"))
    |> Find.to_map(["kind","key","at","salt"])
  end
  defp find_log(data) do
    case data do
      nil -> nil
      update ->
        Find.find_entity(@log_kind, update |> to_log_key)
        |> Find.to_map(["kind","key","at","salt","action","last","next","operator","properties"])
    end
  end

  defp to_log_key(data) do
    case data do
      nil -> nil
      info -> "#{info["kind"]}:#{info["key"]}:#{info["at"]}:#{info["salt"]}"
    end
  end
  defp to_log(data) do
    data
    |> Diplomat.Entity.new(@log_kind,data |> to_log_key)
  end

  defp to_update(data,type) do
    data
    |> Map.put("type", type)
    |> Diplomat.Entity.new(@update_kind,data |> to_update_key(type))
  end
  defp to_update_key(data,type) do
    "#{data["kind"]}:#{data["key"]}:#{type}"
  end
end
