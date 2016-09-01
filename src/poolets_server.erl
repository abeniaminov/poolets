%%%-------------------------------------------------------------------
%%% @author abeniaminov@gmail.com
%%% @copyright (C) 2016
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(poolets_server).

-author("abeniaminov").

-behaviour(gen_server).

-export([demand/1, demand/2, get_back/2, transaction/2,
    transaction/3, start/2,
    start_link/2, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export_type([pool/0]).

-define(TIMEOUT, 5000).
-define(TFree, get(free)).
-define(TWorking, get(working)).
-define(TWaiting, get(waiting)).
-define(TLaunching, get(launching)).
-define(TName, get(poolname)).




-type pool() ::
Name :: atom()  | pid() |
{Name :: atom(), node()} |
{local, Name :: atom()} |
{global, GlobalName :: any()} |
{via, Module :: atom(), ViaName :: any()}.

% Copied from gen:start_ret/0
-type start_ret() :: {'ok', pid()} | 'ignore' | {'error', term()}.

-record(state, {
    workers  = [] :: [pid()],
    overflow = 0 :: non_neg_integer(),
    shutdown = 1000 :: non_neg_integer() | brutal_kill,
    enabled = false
}).


-spec demand(Pool :: pool()) -> pid() | full.
demand(Pool) ->
    demand(Pool,  ?TIMEOUT).



-spec demand(Pool :: pool(), Timeout :: timeout())
        -> pid() | full | disabled | timeout.
demand(Pool, Timeout) ->
    try
        TRef = erlang:make_ref(),
        gen_server:call(Pool, {demand, TRef}, Timeout)
    catch
        _Class:_Reason ->
            gen_server:cast(Pool, {cancel_wait, TRef}),
            {fault, timeout}
    end.


-spec get_back(Pool :: pool(), Worker :: pid()) -> ok.
get_back(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {get_back, Worker}).



-spec transaction(Pool :: pool(), Fun :: fun((Worker :: pid()) -> any()))
        -> any().
transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?TIMEOUT).

-spec transaction(Pool :: pool(), Fun :: fun((Worker :: pid()) -> any()),
    Timeout :: timeout()) -> any().
transaction(Pool, Fun, Timeout) ->
    case demand(Pool, Timeout) of
        {worker, Worker}  ->
            try
                Fun(Worker)
            after
                ok = get_back(Pool, Worker)
            end;
        {fault, Fault} -> {fault, Fault}
    end.


-spec start(PoolArgs :: proplists:proplist(),
    WorkerArgs:: proplists:proplist())
        -> start_ret().
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).


-spec start_link(PoolArgs :: proplists:proplist(),
    WorkerArgs:: proplists:proplist())
        -> start_ret().
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

-spec stop(Pool :: pool()) -> ok.
stop(Pool) ->
    gen_server:call(Pool, stop).


init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    erlang:put(mfa, WorkerArgs),
    init_ets(free),
    init_ets(working),
    init_ets(waiting),
    init_ets(launching),
    init(PoolArgs,  #state{}).

init([{name, Pool} | Rest],  State)  ->
    erlang:put(pool_name, Pool),
    init(Rest, State);
init([{mfa, MFA} | Rest],  State) when is_tuple(MFA) ->
    erlang:put(mfa, MFA),
    init(Rest, State);
init([{init_size, Size} | Rest],  State) when is_integer(Size) ->
    erlang:put(init_size, Size),
    init(Rest,  State);
init([{max_overflow, MaxOverflow} | Rest],  State) when is_integer(MaxOverflow) ->
    erlang:put(max_overflov, MaxOverflow),
    init(Rest,  State);
init([_ | Rest],  State) ->
    init(Rest,  State);
init([],  State) ->
    NewWorkers = prepopulate(),
    {ok, State#state{workers = NewWorkers}}.

handle_call({demand, TRef}, {FromPid, _Tag} = From, state#{enabled = true} = State) ->
    CRef = erlang:monitor(process, FromPid),
    R = try_get(State#state.overflow, TRef, CRef, From),
    case R of
        {worker, Worker}  ->
            {reply, {worker, Worker}, State};
        {wait, CRef} ->
            {noreply, State};
        full ->
            erlang:demonitor(CRef),
            {reply, {fault, full}, State}
    end;



handle_call({demand, _TRef}, _From, state#{enabled = false} = State) ->
    {reply, {fault, disabled}, State};


handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Msg, _From, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, State}.




handle_cast({get_back, Worker}, State) ->
    [{Worker, CRef, _CPid}] = ets:take(?TWorking, Worker),
    give_back(Worker, State#state.enabled),
    true = erlang:demonitor(CRef, [flush]),
    {noreply, State};

handle_cast({cancel_wait, TRef}, State) ->
    TWaiting = ?TWaiting,
    case ets:match_object(TWaiting, {'$1', TRef, '$2', '_'}) of
        [{Key, WaitingClientRef}] ->
            true = demonitor(WaitingClientRef),
            true = ets:delete(TWaiting, Key);
        [] ->
            continue
    end,
    {noreply, State};


handle_cast(_Msg, State) ->
    {noreply, State}.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% delayed part of start workers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_info({new_worker,  LauncherPid, {ok, Worker}}, #state{workers = Workers, enabled = IsEnabled} = State) ->
    link(Worker),
    TLaunching = ?TLaunching,
    InitSize = erlang:get(init_size),
    case ets:lookup(TLaunching, LauncherPid) of
        [{LauncherPid, _TRef, _From}] ->
            true = ets:delete(TLaunching, LauncherPid),
            give_back(Worker, IsEnabled);
        [] -> continue
    end,
    {noreply, State#state{workers = [Worker | Workers], enabled =  (length(Workers) > InitSize-1) }};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% client unexpected dead
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info({'DOWN', CRef, _, _, _}, #state{workers = Workers} = State) ->
    true = ets:match_delete(?TWaiting, {'$1', CRef, '$2', '_'}),
    true = ets:match_delete(?TLaunching, {'$1', '$2', '_' }),
    case ets:match(?TWorking, {'$1', '_' , CRef, '_' }) of
        [[Worker]] ->
            [{Worker, TRef, CRef, From}] = ets:take(?TWorking, Worker),
            shutdown_worker(Worker, brutal_kill),
            NWorkers = lists:filter(fun(P) -> P =/= Worker end, Workers),
            launch_worker(get(mfa), TRef, From),
            {noreply, State#state{workers = NWorkers}};
        [] ->
            {noreply, State}
    end;


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% worker unexpected dead
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info({'EXIT', Worker, _Reason}, #state{workers = Workers} = State) ->
    true = ets:match_delete(?TFree, {'$1', Worker}),
    NWorkers = lists:filter(fun(P) -> P =/= Worker end, Workers),
    case ets:lookup(?TWorking, Worker) of
        [{Worker, TRef, CRef, From}] ->
            erlang:demonitor(CRef, [flush]),
            true = ets:delete(?TWorking, Worker),
            launch_worker(get(mfa), TRef, From);
        [] -> nothing
    end,
    {noreply, State#state{workers = NWorkers}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ok = lists:foreach(fun (W) -> shutdown_worker( W, State#state.shutdown) end, State#state.workers).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            catch (exit ("pool name underfined"));
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.


%% IsEnable = true
give_back(Worker, true) when is_pid(Worker) ->
    case ets:first(?TWaiting) of
        '$end_of_table' ->
            ets:insert_new(?TFree, {erlang:system_time(), Worker} );
        Key ->
            [{Key, TRef, WaitingClientRef, FromClient}] = ets:take(?TWaiting, Key),
            gen_server:reply(FromClient, Worker),
            ets:insert_new(?TWorking, {Worker, TRef, WaitingClientRef, FromClient})
    end;

%% Disabled, IsEnable = false
give_back(Worker, false) when is_pid(Worker) ->
    ets:insert_new(?TFree, {erlang:system_time(), Worker} ).


try_get(Overflow, TRef, CRef, From) ->
    TFree = ?TFree,
    MaxOverflow = get(max_overflow),
    case ets:first(TFree) of
        '$end_of_table' when Overflow < MaxOverflow ->
            ets:insert_new(?TWaiting, {erlang:system_time(), TRef, CRef, From}),
            {wait, CRef};
        '$end_of_table' ->
            full;
        Key ->
            [{Key, Worker}] = ets:take(TFree, Key),
            ets:insert_new(?TWorking, {Worker, TRef, CRef, From}),
            {worker, Worker}
    end.



init_ets(TableName) ->
    erlang:put(TableName, ets:new(TableName, [ordered_set, private])).


launch_worker({M, F, A},  TRef, From ) ->
    Launcher = async_call(M, F, A, new_worker),
    ets:insert_new(?TLaunching, {Launcher, TRef, From }),
    Launcher.

async_call(M, F, A, Action) when is_atom(M), is_atom(F), is_list(A) ->
    SendTo = self(),
    {Pid, _Ref} = spawn( fun() ->
               R = call(M, F, A),
               SendTo ! {Action, self(), R}
           end),
    Pid.

call(M, F, A) ->
    case catch apply(M, F, A) of
        {'EXIT, _'} = V -> {bad_call, V};
        R -> R
    end.


prepopulate() ->
    Size = get(init_size),
    {M, F, A} = get(mfa),
    lists:foreach(fun (_X) -> async_call(M, F, A, new_worker) end, lists:seq(1, Size)).

shutdown_worker(Pid, brutal_kill) ->
    case monitor_worker(Pid) of
        ok ->
            exit(Pid, kill),
            receive
                {'DOWN', _MRef, process, Pid, killed} ->
                    ok;
                {'DOWN', _MRef, process, Pid, OtherReason} ->
                    {error, OtherReason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
shutdown_worker(Pid, Time) ->
    case monitor_worker(Pid) of
        ok ->
            exit(Pid, shutdown), %% Try to shutdown gracefully
            receive
                {'DOWN', _MRef, process, Pid, shutdown} ->
                    ok;
                {'DOWN', _MRef, process, Pid, OtherReason} ->
                    {error, OtherReason}
            after Time ->
                exit(Pid, kill),  %% Force termination.
                receive
                    {'DOWN', _MRef, process, Pid, OtherReason} ->
                        {error, OtherReason}
                end
            end;
        {error, Reason} ->
            {error, Reason}
    end.

monitor_worker(Pid) ->
erlang:monitor(process, Pid),
    unlink(Pid),
    receive
        {'EXIT', Pid, Reason} ->
            receive
                {'DOWN', _, process, Pid, _} ->
                {error, Reason}
            end
    after 0 ->
        ok
end.