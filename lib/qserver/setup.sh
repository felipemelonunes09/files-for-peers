python server.py &
SERVER_PID=$!
sleep 2
python client.py &
CLIENT_PID=$!

sleep 5

kill $SERVER_PID
kill $CLIENT_PID