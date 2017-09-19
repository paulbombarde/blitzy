defmodule Blitzy.CLI do
  require Logger

  def main(args) do
    Application.get_env(:blitzy, :master_node)
    |> Node.start

    Application.get_env(:blitzy, :slave_nodes)
    |> Enum.each(&Node.connect(&1))

    args
    |> parse_args
    |> process_options([node()|Node.list])
  end

  defp parse_args(args) do
    OptionParser.parse(args,
                        aliases: [n: :number],
                        strict: [number: :integer])
  end

  defp process_options(options, nodes) do
    case options do
      {[number: n],[url],[]} ->
        Logger.info "Got n=#{n} and url=#{url}, nodes=#{inspect nodes}"
        do_request(n, url, nodes)
      _->
        do_help()
    end
  end

  defp do_help do
    IO.puts """
    Usage blitzy -n number url
    """
  end

  defp do_request(n, url, nodes) do
    number_nodes=Enum.count(nodes)
    req_per_node=div(n, number_nodes)

    nodes
    |> Enum.flat_map(fn node ->
      1..req_per_node
      |> Enum.map(fn _ -> Task.Supervisor.async({Blitzy.TasksSupervisor, node}, Blitzy.Worker, :query, [url])
      end)
    end)
    |> Enum.map(&Task.await(&1))
    |> Blitzy.Worker.handle_results
  end
end
