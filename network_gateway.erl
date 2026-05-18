-module(network_gateway).
-behaviour(gen_server).

-include_lib("header/logger.hrl").

-export([start_link/0, get_health/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    listen_socket :: gen_tcp:socket() | undefined
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_health() ->
    gen_server:call(?MODULE, check_health).

init([]) ->
    ?LOG_INFO("Network gateway initializing"),

    Port = application:get_env(beacon_core, http_port, 8080),
    ?LOG_DEBUG("Gateway port configured", #{port => Port}),

    SocketOptions = [binary, {reuseaddr, true}, {active, false}],

    case gen_tcp:listen(Port, SocketOptions) of
        {ok, ListenSocket} ->
            ?LOG_NOTICE("Gateway listening on port", #{port => Port}),
            %% Signal the process to immediately jump into its non-blocking accept loop
            self() ! accept_next,
            {ok, #state{listen_socket = ListenSocket}};
        {error, Reason} ->
            ?LOG_ERROR("Gateway failed to bind to port", #{port => Port, reason => Reason}),
            {stop, Reason}
    end.

%% This handles your internal Erlang program-to-program health requests
handle_call(check_health, _From, State) ->
    {reply, {ok, health}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% This handles external HTTP network incoming client connections
handle_info(accept_next, State = #state{listen_socket = ListenSocket}) ->
    case gen_tcp:accept(ListenSocket, 1000) of
        {ok, ClientSocket} ->
            ?LOG_DEBUG("Gateway accepted client connection"),
            spawn(fun() -> handle_client(ClientSocket) end),
            self() ! accept_next,
            {noreply, State};
        {error, timeout} ->
            self() ! accept_next,
            {noreply, State};
        {error, Reason} ->
            ?LOG_ERROR("Gateway accept error", #{reason => Reason}),
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% This handles process termination
%% It closes the listening socket and returns ok
terminate(_Reason, #state{listen_socket = ListenSocket}) ->
    if ListenSocket =/= undefined -> gen_tcp:close(ListenSocket); true -> ok end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal Network Processing Logic
%% ====================================================================

handle_client(Socket) ->
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, Data} ->
            io:format("[DEBUG] Received raw data: ~p~n", [Data]),
            case parse_http(Data) of
                {ok, Method, Path} ->
                    handle_request(Socket, Method, Path);
                _ ->
                    handle_request(Socket, unknown, unknown)
            end;
        {error, Reason} ->
            io:format("[ERROR] Socket error: ~p~n", [Reason]),
            gen_tcp:close(Socket)
    end.

parse_http(<<"GET ", Rest/binary>>) ->
    case binary:split(Rest, <<" ">>) of
        [Path, _] ->
            io:format("[DEBUG] Parsed path: ~p~n", [Path]),
            {ok, 'GET', Path};
        _ ->
            error
    end;
parse_http(_) ->
    error.

handle_request(Socket, 'GET', <<"/health", _/binary>>) ->
    io:format("[DEBUG] Matched /health~n"),

    % Check service status
    DbStatus = check_database_status(),
    PgStatus = check_pg_status(),
    AmqpStatus = check_amqp_status(),

    OverallStatus = case {DbStatus, PgStatus} of
        {ok, ok} -> <<"healthy">>;
        _ -> <<"degraded">>
    end,

    % Build comprehensive health response
    Body = build_health_response(OverallStatus, DbStatus, PgStatus, AmqpStatus),
    ContentLength = integer_to_binary(byte_size(Body)),

    % Professional HTTP response with security headers
    Response = <<"HTTP/1.1 200 OK\r\n"
                 "Content-Type: application/json; charset=utf-8\r\n"
                 "Content-Length: ", ContentLength/binary, "\r\n"
                 "Cache-Control: no-cache, no-store, must-revalidate\r\n"
                 "Pragma: no-cache\r\n"
                 "Expires: 0\r\n"
                 "X-Content-Type-Options: nosniff\r\n"
                 "X-Frame-Options: DENY\r\n"
                 "X-XSS-Protection: 1; mode=block\r\n"
                 "Strict-Transport-Security: max-age=31536000; includeSubDomains\r\n"
                 "Connection: close\r\n"
                 "\r\n",
                 Body/binary>>,
    gen_tcp:send(Socket, Response),
    gen_tcp:close(Socket);

handle_request(Socket, 'GET', <<"/ws", _/binary>>) ->
    io:format("[DEBUG] Matched /ws~n"),
    gen_tcp:close(Socket);

handle_request(Socket, Method, Path) ->
    io:format("[DEBUG] No match for Method: ~p, Path: ~p~n", [Method, Path]),
    NotFound = <<"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\nConnection: close\r\n\r\nNot Found">>,
    gen_tcp:send(Socket, NotFound),
    gen_tcp:close(Socket).

%% Health check helper functions

% Check database connector status
check_database_status() ->
    case pg:get_members(notification_scope, db_workers) of
        [] -> error;
        _ -> ok
    end.

% Check PG (process group) status
check_pg_status() ->
    try
        _ = pg:get_members(notification_scope, db_workers),
        ok
    catch
        _ -> error
    end.

% Check AMQP connectivity status
check_amqp_status() ->
    case application:get_env(beacon_core, amqp_url) of
        {ok, _} -> ok;
        _ -> error
    end.

% Build comprehensive health response JSON
build_health_response(OverallStatus, DbStatus, PgStatus, AmqpStatus) ->
    Timestamp = get_timestamp(),
    Uptime = get_uptime(),
    Memory = get_memory_usage(),

    StatusMap = iolist_to_binary([
        <<"{">>,
        <<"\"timestamp\":\"">>, Timestamp, <<"\",">>,
        <<"\"status\":\"">>, OverallStatus, <<"\",">>,
        <<"\"uptime_seconds\":">>, Uptime, <<",">>,
        <<"\"services\":{">>,
        <<"\"database\":\"">>, atom_to_binary(DbStatus, utf8), <<"\",">>,
        <<"\"process_group\":\"">>, atom_to_binary(PgStatus, utf8), <<"\",">>,
        <<"\"amqp\":\"">>, atom_to_binary(AmqpStatus, utf8), <<"\"">>,
        <<"},">>,
        <<"\"memory_mb\":">>, Memory, <<",">>,
        <<"\"version\":\"1.0.0\",">>,
        <<"\"node\":\"">>, atom_to_binary(node(), utf8), <<"\"">>,
        <<"}">>
    ]),
    iolist_to_binary(StatusMap).

% Get current timestamp in ISO 8601 format
get_timestamp() ->
    {Date, Time} = calendar:universal_time(),
    DateStr = format_date(Date),
    TimeStr = format_time(Time),
    iolist_to_binary([DateStr, <<"T">>, TimeStr, <<"Z">>]).

format_date({Y, M, D}) ->
    io_lib:format("~4..0B-~2..0B-~2..0B", [Y, M, D]).

format_time({H, Mi, S}) ->
    io_lib:format("~2..0B:~2..0B:~2..0B", [H, Mi, S]).

% Get uptime in seconds
get_uptime() ->
    {TotalMicros, _} = erlang:statistics(wall_clock),
    TotalSeconds = TotalMicros div 1000000,
    integer_to_binary(TotalSeconds).

% Get memory usage in MB
get_memory_usage() ->
    Total = erlang:memory(total),
    MB = Total div (1024 * 1024),
    integer_to_binary(MB).