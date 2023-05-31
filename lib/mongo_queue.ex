defmodule MongoQueue do
  @moduledoc """
  A queue implementation using MongoDB.

  Inspired by the [NPM mongodb-queue package](https://www.npmjs.com/package/mongodb-queue).

  ## Getting Started

  ### Config

  To use MongoQueue, you must first create a queue configuration.
  This configuration requires a connection to a MongoDB database, and the name of the collection to use for the queue.

  To connect to Mongo, consult the [`mongodb_driver` docs](https://hexdocs.pm/mongodb_driver/readme.html).

  For example:

  ```elixir
  {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/my-database")
  ```

  Then, create a queue configuration:

  ```elixir
  config = MongoQueue.Config.new(conn, "my_queue")
  ```

  Configuration options are described in detail in the `MongoQueue.Config` module.

  ### Creating Indexes

  To ensure high performance for the queue, you create indexes on the queue collection.
  This only needs to be run once per queue.

  ```elixir
  :ok = MongoQueue.create_indexes(config)
  ```

  ### Adding Messages

  To add a message to the queue, use the `add/2` function:

  ```elixir
  {:ok, message_id} = MongoQueue.add(config, %{foo: "bar"})
  ```

  ### Receiving Messages

  To receive a message from the queue, use the `get/1` function:

  ```elixir
  {:ok, message} = MongoQueue.get(config)
  ```

  By default, messages are claimed for 30 seconds.
  If they are not acknowledged within that time, they will become available to be received again.

  To customize this timeout, use the `visibility_timeout` option:

  ```elixir
  {:ok, message} = MongoQueue.get(config, visibility_timeout: 60)
  ```

  ### Acknowledging Messages

  When the message has finished processing, call the `ack/2` function:

  ```elixir
  :ok = MongoQueue.ack(config, message.ack)
  ```

  ## Working in bulk

  ### Adding Messages

  To add multiple messages to the queue, use the `add_many/2` function:

  ```elixir
  {:ok, message_ids} = MongoQueue.add_many(config, [%{foo: "bar"}, %{foo: "baz"}])
  ```

  ### Receiving Messages

  To receive multiple messages from the queue, use the `get_many/2` function:

  ```elixir
  {:ok, messages} = MongoQueue.get_many(config, 2)
  ```

  Note: This function performs a multi-document transaction. See the warning on the `get_many/2` function for more information.

  ### Acknowledging Messages

  To acknowledge multiple messages, use the `ack/2` function, with a list of ack IDs:

  ```elixir
  :ok = MongoQueue.ack(config, [message1.ack, message2.ack])
  ```

  # Statistics

  A few methods are provided to help you observe the state of the queue.

  ### Total Messages

  The `total/1` function returns the number of messages that have been added to the queue, regardless of their status.
  This will not include messages that have been deleted by `clean/1`.

  ### Size

  The `size/1` function returns the number of messages that are currently enqueued and visible.

  ### In Flight

  The `in_flight/1` function returns the number of messages that have been received from the queue but not yet acknowledged.

  ### Done

  The `done/1` function returns the number of messages that have been acknowledged but not yet deleted.

  ## Cleaning Up

  From time to time, it may be necessary to clean up the queue collection.
  To do this, run the `clean/1` function.

  This function deletes all messages that have been acknowledged.
  """

  alias Mongo.UnorderedBulk

  defmodule Config do
    @moduledoc """
    Configuration for a queue.

    ## Struct Properties

    * `conn` - A MongoDB connection, from the [mongodb_driver package](https://hex.pm/packages/mongodb_driver).
    * `collection` - The name of the collection to use for the queue.
    * `visibility_timeout` - The amount of time in seconds that a message is invisible after it has been received from the queue.
    * `delay` - The amount of time in seconds that a message is delayed before it is available to be received from the queue.
    """

    defstruct [:conn, :collection, visibility_timeout: 30, delay: 0]

    @type t :: %__MODULE__{
            conn: Mongo.conn(),
            collection: binary(),
            visibility_timeout: non_neg_integer(),
            delay: non_neg_integer()
          }

    @doc """
    Creates a new queue configuration.

    ## Examples

        iex> MongoQueue.Config.new(conn, "my_queue")
        %MongoQueue.Config{
          conn: #PID<0.123.0>,
          collection: "my_queue",
          visibility_timeout: 30,
          delay: 0
        }
    """
    @spec new(Mongo.conn(), binary(), Keyword.t()) :: t()
    def new(conn, collection, opts \\ []) do
      %__MODULE__{conn: conn, collection: collection} |> merge(opts)
    end

    @doc """
    Merges the given options into the configuration.

    ## Examples

        iex> MongoQueue.Config.new(conn, "my_queue") |> MongoQueue.Config.merge(visibility_timeout: 60)
        %MongoQueue.Config{
          conn: #PID<0.123.0>,
          collection: "my_queue",
          visibility_timeout: 60,
          delay: 0
        }
    """
    @spec merge(t(), Keyword.t() | map()) :: t()
    def merge(config, opts) when is_list(opts) do
      opts = Map.new(opts)

      merge(config, opts)
    end

    def merge(config, opts) when is_map(opts) do
      Map.merge(config, opts)
    end
  end

  @doc """
  Adds a message to the queue.

  ## Examples

      iex> MongoQueue.add(config, %{foo: "bar"})
      {:ok, #BSON.ObjectId<5f0e1e1b0000000000000000>}

      iex> MongoQueue.add(config, %{foo: "bar"}, delay: 60)
      {:ok, #BSON.ObjectId<5f0e1e1b0000000000000000>}
  """
  @spec add(Config.t(), any(), Keyword.t()) :: {:ok, BSON.ObjectId.t()} | {:error, any()}
  def add(%Config{} = config, payload, opts \\ []) do
    with {:ok, [id]} <- add_many(config, [payload], opts) do
      {:ok, id}
    end
  end

  @doc """
  Adds multiple messages to the queue.

  ## Examples

      iex> MongoQueue.add_many(config, [%{foo: "bar"}, %{foo: "baz"}])
      {:ok, [#BSON.ObjectId<5f0e1e1b0000000000000000>, #BSON.ObjectId<5f0e1e1b0000000000000001>]}

      iex> MongoQueue.add_many(config, [%{foo: "bar"}, %{foo: "baz"}], delay: 60)
      {:ok, [#BSON.ObjectId<5f0e1e1b0000000000000000>, #BSON.ObjectId<5f0e1e1b0000000000000001>]}
  """
  @spec add_many(Config.t(), Enumerable.t(any()), Keyword.t()) ::
          {:ok, [BSON.ObjectId.t()]} | {:error, any()}
  def add_many(%Config{} = config, payloads, opts \\ []) do
    config = Config.merge(config, opts)

    visible = DateTime.utc_now() |> DateTime.add(config.delay)

    payloads = Enum.map(payloads, fn payload -> %{visible: visible, payload: payload} end)

    with {:ok, %Mongo.InsertManyResult{inserted_ids: inserted_ids}} <-
           Mongo.insert_many(config.conn, config.collection, payloads) do
      {:ok, inserted_ids}
    end
  end

  @type message :: %{id: BSON.ObjectId.t(), ack: BSON.ObjectId.t(), payload: any()}

  @doc """
  Gets a payload from the queue.

  ## Examples

      iex> MongoQueue.get(config)
      {:ok, %{
        id: #BSON.ObjectId<5f0e1e1b0000000000000000>,
        ack: #BSON.ObjectId<5f0e1e1b0000000000000001>,
        payload: %{foo: "bar"}
      }}

      iex> MongoQueue.get(config)
      {:ok, nil}
  """
  @spec get(Config.t(), Keyword.t()) ::
          {:ok, message()}
          | {:ok, nil}
          | {:error, any()}
  def get(%Config{} = config, opts \\ []) do
    config = Config.merge(config, opts)

    query = %{deleted: nil, visible: %{"$lte" => DateTime.utc_now()}}

    update = %{
      "$set" => %{
        ack: Mongo.object_id(),
        visible: DateTime.utc_now() |> DateTime.add(config.visibility_timeout)
      }
    }

    result =
      Mongo.find_one_and_update(config.conn, config.collection, query, update,
        sort: %{_id: 1},
        return_document: :after
      )

    case result do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, doc} ->
        {:ok,
         %{
           id: doc["_id"],
           ack: doc["ack"],
           payload: doc["payload"]
         }}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Gets multiple messages from the queue.

  > #### Warning {: .warning}
  > This operation performs a multi-document transaction.
  > In MongoDB, this requires a replica set.
  > To learn more, check out [MongoDB’s documentation](https://www.mongodb.com/docs/manual/replication/#transactions).

  ## Parameters

  * `config` - A queue configuration.
  * `count` - The maximum number of messages to retrieve. If fewer than this number of messages are available, a smaller amount will be received.
  * `opts` - Additional options, to override those found in the `config` parameter.

  ## Examples

      iex> MongoQueue.get_many(config, 2)
      {:ok, [
        %{
          id: #BSON.ObjectId<5f0e1e1b0000000000000000>,
          ack: #BSON.ObjectId<5f0e1e1b0000000000000001>,
          payload: %{foo: "bar"}
        },
        %{
          id: #BSON.ObjectId<5f0e1e1b0000000000000002>,
          ack: #BSON.ObjectId<5f0e1e1b0000000000000003>,
          payload: %{foo: "baz"}
        }
      ]}

      iex> MongoQueue.get_many(config, 2)
      {:ok, []}
  """
  @spec get_many(Config.t(), non_neg_integer(), Keyword.t()) ::
          {:ok, [message()]}
          | {:error, any()}
  def get_many(%Config{} = config, count, opts \\ []) do
    config = Config.merge(config, opts)

    query = %{deleted: nil, visible: %{"$lte" => DateTime.utc_now()}}

    Mongo.transaction(
      config.conn,
      fn ->
        with docs when not is_tuple(docs) <-
               Mongo.find(config.conn, config.collection, query,
                 limit: count,
                 sort: %{_id: 1}
               ),
             docs <- Enum.to_list(docs) do
          doc_count = length(docs)
          ack_ids = Enum.map(docs, fn _ -> Mongo.object_id() end)
          ids = Enum.map(docs, fn doc -> doc["_id"] end)
          visible = DateTime.utc_now() |> DateTime.add(config.visibility_timeout)

          bulk =
            Enum.zip(ids, ack_ids)
            |> Enum.reduce(UnorderedBulk.new(config.collection), fn {id, ack_id}, bulk ->
              UnorderedBulk.update_one(bulk, %{_id: id}, %{
                "$set" => %{
                  ack: ack_id,
                  visible: visible
                }
              })
            end)

          case Mongo.BulkWrite.write(config.conn, bulk) do
            %Mongo.BulkWriteResult{modified_count: ^doc_count} ->
              results =
                Enum.zip(docs, ack_ids)
                |> Enum.map(fn {doc, ack_id} ->
                  %{
                    id: doc["_id"],
                    ack: ack_id,
                    payload: doc["payload"]
                  }
                end)

              {:ok, results}

            %{errors: errors} ->
              {:error, errors}
          end
        end
      end,
      w: :majority,
      read_concern: %{level: :linearizable}
    )
  end

  @doc """
  Marks a message as acknowledged, making it unavailable to be received from the queue in the future.

  Acknowledged messages may eventually be deleted, using the `clean/1` function.

  ## Parameters

  * `config` - A queue configuration.
  * `ack_or_acks` - The ack ID of the message to acknowledge, or a list of ack IDs to acknowledge.
  * `opts` - Additional options, to override those found in the `config` parameter.

  ## Examples

      iex> MongoQueue.ack(config, ack)
      :ok

      iex> MongoQueue.ack(config, [ack1, ack2])
      :ok
  """
  @spec ack(Config.t(), BSON.ObjectId.t() | [BSON.ObjectId.t()], Keyword.t()) ::
          :ok | {:error, any()}
  def ack(config, ack_or_acks, opts \\ [])

  def ack(%Config{} = config, acks, opts) when is_list(acks) do
    config = Config.merge(config, opts)

    query = %{ack: %{"$in" => acks}, visible: %{"$gt" => DateTime.utc_now()}, deleted: nil}

    update = %{
      "$set" => %{deleted: DateTime.utc_now()}
    }

    with {:ok, %Mongo.UpdateResult{}} <-
           Mongo.update_many(config.conn, config.collection, query, update) do
      :ok
    end
  end

  def ack(%Config{} = config, ack, opts) do
    config = Config.merge(config, opts)

    query = %{ack: ack, visible: %{"$gt" => DateTime.utc_now()}, deleted: nil}

    update = %{
      "$set" => %{deleted: DateTime.utc_now()}
    }

    with {:ok, doc} when not is_nil(doc) <-
           Mongo.find_one_and_update(config.conn, config.collection, query, update) do
      :ok
    end
  end

  @doc """
  Marks a message as not acknowledged, making it immediately available to be received from the queue again.

  ## Examples

      iex> MongoQueue.nack(config, ack)
      :ok
  """
  @spec nack(Config.t(), BSON.ObjectId.t(), Keyword.t()) :: :ok | {:error, any()}
  def nack(%Config{} = config, ack, opts \\ []) do
    config = Config.merge(config, opts)

    query = %{ack: ack, visible: %{"$gt" => DateTime.utc_now()}, deleted: nil}

    update = %{
      "$set" => %{
        visible: DateTime.utc_now()
      },
      "$unset" => %{ack: ""}
    }

    with {:ok, doc} when not is_nil(doc) <-
           Mongo.find_one_and_update(config.conn, config.collection, query, update) do
      :ok
    end
  end

  @doc """
  Extends the visibility timeout of a received message.

  This allows long-running tasks to operate on a message without the risk of it being received by another process.

  ## Examples

      iex> MongoQueue.ping(config, ack)
      :ok
  """
  def ping(%Config{} = config, ack, opts \\ []) do
    config = Config.merge(config, opts)

    query = %{
      ack: ack,
      visible: %{"$gt" => DateTime.utc_now()},
      deleted: nil
    }

    update = %{
      "$set" => %{
        visible: DateTime.utc_now() |> DateTime.add(config.visibility_timeout)
      }
    }

    with {:ok, doc} when not is_nil(doc) <-
           Mongo.find_one_and_update(config.conn, config.collection, query, update) do
      :ok
    end
  end

  @doc """
  Removes acknowledged documents from the queue collection.

  Messages remain in the MongoDB collection until they are deleted by this function.
  """
  def clean(%Config{} = config) do
    Mongo.delete_many(config.conn, config.collection, %{deleted: %{"$exists" => true}})
  end

  @doc """
  Returns the total number of messages in the queue’s collection—regardless of their status.
  """
  def total(%Config{} = config) do
    Mongo.count_documents(config.conn, config.collection, %{})
  end

  @doc """
  Returns the number of enqueued, visible messages in the queue’s collection.
  """
  def size(%Config{} = config) do
    query = %{
      deleted: nil,
      visible: %{"$lte" => DateTime.utc_now()}
    }

    Mongo.count_documents(config.conn, config.collection, query)
  end

  @doc """
  Returns the number of messages that have been received from the queue but not yet acknowledged.
  """
  def in_flight(%Config{} = config) do
    query = %{
      ack: %{"$exists" => true},
      visible: %{"$gt" => DateTime.utc_now()},
      deleted: nil
    }

    Mongo.count_documents(config.conn, config.collection, query)
  end

  @doc """
  Returns the number of messages that have been acknowledged but not yet deleted.
  """
  def done(%Config{} = config) do
    query = %{
      deleted: %{"$exists" => true}
    }

    Mongo.count_documents(config.conn, config.collection, query)
  end

  @doc """
  Creates indexes to ensure high performance for the queue.

  This only needs to be run once per queue.
  """
  def create_indexes(%Config{} = config) do
    Mongo.create_indexes(config.conn, config.collection, [
      [key: [deleted: 1, visible: 1], name: "deleted_visible"],
      [key: [ack: 1], unique: true, sparse: true, name: "ack"]
    ])
  end
end
