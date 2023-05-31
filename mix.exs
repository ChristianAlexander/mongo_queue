defmodule MongoQueue.MixProject do
  use Mix.Project

  @version "0.0.3"

  def project do
    [
      app: :mongo_queue,
      version: @version,
      start_permanent: Mix.env() == :prod,
      package: package(),
      deps: deps(),
      docs: docs(),
      name: "MongoQueue",
      source_url: "https://github.com/ChristianAlexander/mongo_queue",
      description: "A simple queue backed by MongoDB"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:mongodb_driver, "~> 1.0"},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/ChristianAlexander/mongo_queue"
      },
      maintainers: ["Christian Alexander"]
    ]
  end

  defp docs do
    [
      main: "MongoQueue",
      source_ref: @version
    ]
  end
end
