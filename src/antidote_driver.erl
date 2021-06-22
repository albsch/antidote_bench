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
operations() -> {ok, [{update_only_txn, 1}]}.
test_dir() -> {ok, "tests"}.
key_generator() -> {ok, {uniform_int, 100000}}.
value_generator() -> {ok, {fixed_bin, 100}}.
random_algorithm() -> {ok, exsss}.
random_seed() -> {ok, {1,4,3}}.
shutdown_on_error() -> false.

% antidote config 
%%{operations, [{update_only_txn, 1}, {read_only_txn, 1}, {append, 1}, {read, 1}, {txn, 1} ]}.
antidote_pb_port() -> 8087.
antidote_pb_ip() -> '127.0.0.1'.
antidote_types() -> dict:from_list([{antidote_crdt_counter_pn, [{increment,1}, {decrement,1}]}]).
%%{antidote_types, [{antidote_crdt_set_aw, [add, remove]}]}.
%%{antidote_types, [{antidote_crdt_set_go, [add, remove]}]}.
%%{antidote_types, [{antidote_crdt_register_mv, [assign]}]}.
%%{antidote_types, [{antidote_crdt_register_lww, [assign]}]}.
%% Use the following parameter to set the size of the orset
set_size() -> 10.


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

-define(BUCKET, <<"antidote_bench_bucket">>).

-record(state, {worker_id,
                time,
                pb_pid,
                last_read,
                commit_time
                }).

new(Id) ->
  io:format(user, "~nInitializing antidote bench worker~n=====================~n", []),

  io:format(user, "Using target node ~p for worker ~p", [antidote_pb_ip(), Id]),

  {ok, Pid} = antidotec_pb_socket:start_link(antidote_pb_ip(), antidote_pb_port()),
  io:format(user, "Connection established", []),

  {ok, #state{
    worker_id = Id,
    time = {1, 1, 1},
    pb_pid = Pid,
    last_read = {undefined, undefined},
    commit_time = ignore
  }}.


%% @doc This transaction will only perform update operations,
%% by calling the static update_objects interface of antidote.
%% the number of operations is defined by the {num_updates, x}
%% parameter in the config file.
run(update_only_txn, KeyGen, ValueGen, State=#state{pb_pid=Pid, worker_id=Id, commit_time=OldCommitTime})->
  try
    {ok, {static, {TimeStamp, TxnProperties}}} = antidotec_pb:start_transaction(Pid, OldCommitTime, [{static, true}]),
    UpdateIntKeys = generate_keys(num_updates(), KeyGen),
    BObjs = multi_get_random_param_new(UpdateIntKeys, antidote_types(), ValueGen(), undefined, set_size()),
    ok = create_update_operations(Pid, BObjs, {static, {TimeStamp, TxnProperties}}, sequential_writes()),
    {ok, BCommitTime} = antidotec_pb:commit_transaction(Pid, {static, {TimeStamp, TxnProperties}}),
    {ok, State#state{commit_time=BCommitTime}}
  catch _:R:S  -> {error, {R, S}, State}
  end.

terminate(_, _) -> ok.




create_update_operations(_Pid, [], _TxInfo, _IsSeq) ->
  ok;
create_update_operations(Pid, BoundObjects, TxInfo, IsSeq) ->
  case IsSeq of
    true ->
      lists:map(fun(BoundObj) ->
        antidotec_pb:update_objects(Pid, [BoundObj], TxInfo)
                end, BoundObjects),
      ok;
    false ->
      antidotec_pb:update_objects(Pid, BoundObjects, TxInfo)
  end.

%% @doc generate NumReads unique keys using the KeyGen
generate_keys(NumKeys, KeyGen) ->
  Seq = lists:seq(1, NumKeys),
  S = lists:foldl(fun(_, Set) ->
    N = unikey(KeyGen, Set),
    sets:add_element(N, Set)
                  end, sets:new(), Seq),
  sets:to_list(S).

unikey(KeyGen, Set) ->
  R = KeyGen(),
  case sets:is_element(R, Set) of
    true ->
      unikey(KeyGen, Set);
    false ->
      R
  end.

get_key_type(Key, Dict) ->
  Keys = dict:fetch_keys(Dict),
  RanNum = Key rem length(Keys),
  lists:nth(RanNum+1, Keys).

multi_get_random_param_new(KeyList, Dict, Value, Objects, SetSize) ->
  multi_get_random_param_new(KeyList, Dict, Value, Objects, SetSize, []).

multi_get_random_param_new([], _Dict, _Value, _Objects, _SetSize, Acc)->
  Acc;
multi_get_random_param_new([Key|Rest], Dict, Value, Objects, SetSize, Acc)->
  Type = get_key_type(Key, Dict),
  case Objects of
    undefined ->
      Obj = undefined,
      ObjRest = undefined;
    [H|T] ->
      Obj = H,
      ObjRest = T
  end,
  [Param] = get_random_param_new(Key, Dict, Type, Value, Obj, SetSize),
  multi_get_random_param_new(Rest, Dict, Value, ObjRest, SetSize, [Param|Acc]).

get_random_param_new(Key, Dict, Type, Value, Obj, SetSize)->
  Params=dict:fetch(Type, Dict),
  Num=rand:uniform(length(Params)),
  BKey=list_to_binary(integer_to_list(Key)),
  NewVal=case Value of
           Value when is_integer(Value)->
             integer_to_list(Value);
           Value when is_binary(Value)->
             Value
         end,
  case Type of
    antidote_crdt_counter_pn->
      case lists:nth(Num, Params) of
        {increment, Ammount}->
          [{{BKey, Type, ?BUCKET}, increment, Ammount}];
        {decrement, Ammount}->
          [{{BKey, Type, ?BUCKET}, decrement, Ammount}];
        increment->
          [{{BKey, Type, ?BUCKET}, increment, 1}];
        decrement->
          [{{BKey, Type, ?BUCKET}, decrement, 1}]
      end;

    RegisterType when ((RegisterType==antidote_crdt_register_mv) orelse (RegisterType==antidote_crdt_register_lww))->
      [{{BKey, Type, ?BUCKET}, assign, NewVal}];

    SetType when ((SetType==antidote_crdt_set_aw) orelse (SetType==antidote_crdt_set_go))->
      Set=
        case Obj of
          undefined->
            [];
          _ ->
            antidotec_set:value(Obj)
        end,
      %%Op = lists:nth(Num, Params),
      NewOp=case length(Set)=<SetSize of
              true->
                add;
              false->
                remove
            end,
      case NewOp of
        remove->
          case Set of
            []->
              [{{BKey, Type, ?BUCKET}, add_all, [NewVal]}];
            _ ->
              [{{BKey, Type, ?BUCKET}, remove_all, [lists:nth(rand:uniform(length(Set)), Set)]}]
          end;
        _->
          [{{BKey, Type, ?BUCKET}, add_all, [NewVal]}]
      end
  end.
