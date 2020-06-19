-module(emqx_mysql).

-include("emqx_mysql.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SAVE_MESSAGE_PUBLISH, <<"INSERT INTO mqtt_msg(`mid`, `client_id`, `topic`, `payload`, `time`) VALUE(?, ?, ?, ?, ?);">>).
-define(CLIENT_CONNECTED_SQL,
    <<"insert into mqtt_client(clientid, state, "
                   "node, online_at, offline_at) values(?, "
                   "?, ?, now(), null) on duplicate key "
                   "update state = null, node = ?, online_at "
                   "= now(), offline_at = null">>).
-define(CLIENT_DISCONNECTED_SQL,
                 <<"update mqtt_client set state = ?, offline_at "
                   "= now() where clientid = ?">>).
-export([load_hook/1, unload_hook/0, on_message_publish/2, on_client_connected/3, on_client_disconnected/4]).


load_hook(Env) ->
	emqx:hook('client.connected',    {?MODULE, on_client_connected/3, [Env]}),
    	emqx:hook('client.disconnected', {?MODULE, on_client_disconnected/4, [Env]}),
	emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

unload_hook() ->
	emqx:unhook('client.connected',    {?MODULE, on_client_connected/3}),
    	emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected/4}),
	emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).

on_client_connected(ClientInfo = #{clientid := ClientId, peerhost := Peerhost}, ConnInfo, _Env) ->
    emqx_mysql_cli:query(?CLIENT_CONNECTED_SQL, [binary_to_list(ClientId),null,tuple_to_list(Peerhost),null]),
    %%io:format("Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n, Peerhost:~n~p~n", [ClientId, ClientInfo, ConnInfo, Peerhost]),
    ok.

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
    emqx_mysql_cli:query(?CLIENT_DISCONNECTED_SQL, [null,binary_to_list(ClientId)]),
    %%io:format("Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",[ClientId, ReasonCode, ClientInfo, ConnInfo]),
    ok.


on_message_publish(#message{from = emqx_sys} = Message, _State) ->
	{ok, Message};
on_message_publish(#message{} = Message, _State) ->
	#message{id = Id, topic = Topic, payload = Payload, from = From} = Message,
	emqx_mysql_cli:query(?SAVE_MESSAGE_PUBLISH, [emqx_guid:to_hexstr(Id), binary_to_list(From), binary_to_list(Topic), binary_to_list(Payload), timestamp()]),
	{ok, Message};
on_message_publish(Message, _State) ->
	{ok, Message}.

timestamp() ->
	{A,B,_C} = os:timestamp(),
	A*1000000+B.
