-module(antidote_driver).
-behaviour(rcl_bench_driver).

-export([new/1, run/4, terminate/2]).

-export([mode/0, concurrent_workers/0, duration/0, operations/0, test_dir/0,
         key_generator/0, value_generator/0, random_algorithm/0,
         random_seed/0, shutdown_on_error/0]).
% generic config 
mode() -> {ok, {rate, max}}.
concurrent_workers() -> {ok, 2}.
duration() -> {ok, 1}.
operations() -> {ok, [{get_own_puts, 3}, {put, 10}, {get, 2}]}.  
test_dir() -> {ok, "tests"}.
key_generator() -> {ok, {uniform_int, 100000}}.
value_generator() -> {ok, {fixed_bin, 100}}.
random_algorithm() -> {ok, exsss}.
random_seed() -> {ok, {1,4,3}}.
shutdown_on_error() -> false.

% antidote config 
%%{operations, [{update_only_txn, 1}, {read_only_txn, 1}, {append, 1}, {read, 1}, {txn, 1} ]}.
antidote_pb_port() -> 8087.
antidote_pb_ips() -> ['127.0.0.1'].
antidote_types() -> [{antidote_crdt_counter_pn, [{increment,1}, {decrement,1}]}].
%%{antidote_types, [{antidote_crdt_set_aw, [add, remove]}]}.
%%{antidote_types, [{antidote_crdt_set_go, [add, remove]}]}.
%%{antidote_types, [{antidote_crdt_register_mv, [assign]}]}.
%%{antidote_types, [{antidote_crdt_register_lww, [assign]}]}.
%% Use the following parameter to set the size of the orset
%set_size() -> 10.


%%%%%%%%%%%%%%%%%%%%%%%%%%%% for transacitons %%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The following parameters are used when issuing transactions.
%% When running append and read operations, they are ignored.

%% Number of reads; update_only_txn ignores it.
num_reads() -> 10.
%% Number of updates; read_only_txn ignores it.
num_updates() -> 10.

%% If sequential_reads is set to true,
%% the client will send each read (of a total
%% num_reads) in a different antidote:read_objects call.
%% when set to false, all (num_reads) reads will be sent
%% in a single read_objects call, which is faster, as
%% antidote will process them in parallel.
sequential_reads() -> false.

%% Idem for updates
sequential_writes() -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%% end for transacitons %%%%%%%%%%%%%%%%%%%%%%%%%%%%


-record(state, {worker_id,
                time,
                pb_pid,
                last_read,
                commit_time
                }).

new(Id) ->
    io:format(user, "~nInitializing antidote bench worker~n=====================~n", []),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(antidote_pb_ips())+1), antidote_pb_ips()),
    io:format(user, "Using target node ~p for worker ~p", [TargetNode, Id]),

    {ok, Pid} = {ok,ok},%antidotec_pb_socket:start_link(TargetNode, TargetPort),
    {ok, #state{
            worker_id = Id,
            time = {1, 1, 1}, 
            pb_pid = Pid,
            last_read = {undefined, undefined},
            commit_time = ignore
    }}.

run(get, KeyGen, _ValueGen, State) ->
    io:format(user, "get~n", []),
    {ok, state};
run(put, KeyGen, ValueGen, State) ->
    io:format(user, "put~n", []),
    {ok, state};
run(get_own_puts, _KeyGen, _ValueGen, State) ->
    io:format(user, "get_own_puts~n", []),
    {ok, state}.

terminate(_, _) -> ok.









