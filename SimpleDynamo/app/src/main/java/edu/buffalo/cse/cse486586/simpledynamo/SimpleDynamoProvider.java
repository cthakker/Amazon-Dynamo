package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;


public class SimpleDynamoProvider extends ContentProvider {

	final static String TAG = SimpleDynamoActivity.class.getSimpleName();
	private static int SERVER_PORT = 10000;
	TreeMap<String, String> tMap;

	String myPort;
	String succ1;
	ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
	ConcurrentHashMap<String, String> result;
	ConcurrentHashMap<Integer, Object> lockMap;
	int messageID = 0;
	Object allQLock;


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public boolean onCreate() {

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)*2));
		Log.e(TAG, "created "+myPort);

		allQLock = new Object();
		result = new ConcurrentHashMap<String, String>();
		lockMap = new ConcurrentHashMap<Integer, Object>();



		/******** TreeMap **********/
		tMap = new TreeMap<>();
		try {
			Log.i(TAG, "Adding Nodes");
			tMap.put(genHash("5554"), "11108");
			tMap.put(genHash("5556"), "11112");
			tMap.put(genHash("5558"), "11116");
			tMap.put(genHash("5560"), "11120");
			tMap.put(genHash("5562"), "11124");

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		switch(myPort)
		{
			case "11108":
				succ1="11116";
				break;
			case "11112":
				succ1="11108";
				break;
			case "11116":
				succ1="11120";
				break;
			case "11120":
				succ1="11124";
				break;
			case "11124":
				succ1="11112";
				break;
			default:
				Log.e(TAG,"Error in finding port no:"+myPort);

		}

		// Server created
		try
		{
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,new ServerSocket(SERVER_PORT));
			Log.i(TAG,"Server created");
		}
		catch (IOException e){
			e.printStackTrace();
		}
		return true;
	}

	private class ServerTask extends AsyncTask<ServerSocket, Message,  Void> {

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			Socket socket;
			BufferedInputStream bufferedInputStream;
			ObjectInputStream objectInputStream;

			while (true){
				try
				{
					socket = serverSocket.accept();
					bufferedInputStream = new BufferedInputStream(socket.getInputStream());
					objectInputStream = new ObjectInputStream(bufferedInputStream);
					Message m = (Message) objectInputStream.readObject();
					publishProgress(m);
					objectInputStream.close();
					socket.close();

				}
				catch (IOException e){
					e.printStackTrace();
				}
				catch (ClassNotFoundException e){
					e.printStackTrace();
				}

			}

		}
		protected void onProgressUpdate(Message... msgs){
			Message m = msgs[0];
			String msgType=m.getMsgType();
			Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

			if(msgType.equals("Copy")){
				Log.i(TAG,"Copying of key"+m.getKey());
				map.put(m.getKey(),m.getValue());
			}
			else if (msgType.equals("AllQ")) {

				HashMap<String, String> result2;
				result2 = m.getResult();

				if (m.getRemotePort().equals(m.getSenderPort())) {
					Log.i(TAG, "Entered equality");

					for (Map.Entry<String, String> me : map.entrySet()) {
						result2.put(me.getKey(), me.getValue());
					}
					m.setMsgType("ReplyAllQ");
					m.setResult(result2);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);
				} else {

					for (Map.Entry<String, String> me : map.entrySet()) {
						Log.i(TAG, " Entryvalues " + me.getKey() + " " + me.getValue());
						result2.put(me.getKey(), me.getValue());
					}
					m.setResult(result2);
					m.setRemotePort(succ1);
					Log.i(TAG, "Forwarding to the successorport " + m.getRemotePort());
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);

				}
			}
			else if (msgType.equals("ReplyAllQ")){
				result.putAll(m.getResult());
				Log.i(TAG,"Got replies from all nodes.");
				synchronized (allQLock){
					allQLock.notify();
					Log.i(TAG,"Lock notified");
				}
			}
			else if(msgType.equals("SingleQ")){
				Log.i(TAG, "Single Query request received.");
				String key0 = m.getKey();
				String value0 = map.get(key0);
				if(value0!=null){
					Message message = new Message();
					message.setSenderPort(myPort);
					message.setRemotePort(m.getSenderPort());
					message.setMsgType("ReplySingleQ");
					message.setKey(m.getKey());
					message.setMessagID(m.getMessagID());
					message.setValue(value0);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

				}
			}
			else if(msgType.equals("ReplySingleQ")){
				result = new ConcurrentHashMap<String,String>();
				result.put(m.getKey(),m.getValue());
				Log.i(TAG, " key during lock release " + m.getKey());
				Log.i(TAG," value during lock release: "+m.getValue());
				Object lock = lockMap.get(m.getMessagID());
				synchronized (lock){
					lock.notify();
					Log.i(TAG," Released lockID: "+m.getMessagID());
				}

			}
		}

	}
	private class ClientTask extends AsyncTask<Message, Void, Void>{
		@Override
		protected Void doInBackground(Message... msgs) {
			Message msg = msgs[0];
			ObjectOutputStream objectOutputStream;
			BufferedOutputStream bufferedOutputStream;
			Socket socket;
			{
				try{
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg.getRemotePort()));
					bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
					objectOutputStream = new ObjectOutputStream(bufferedOutputStream);
					objectOutputStream.writeObject(msg);
					objectOutputStream.flush();
					objectOutputStream.close();
					socket.close();

				}
				catch (UnknownHostException e){
					e.printStackTrace();
				}
				catch (IOException e){
					e.printStackTrace();
				}
			}
			return null;
		}
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private String getCoordinator(String key){
		try {
			String hash = genHash(key);

			for(Map.Entry<String,String> entry : tMap.entrySet()) {
				String key1 = entry.getKey();
				if (hash.compareTo(key1) <= 0) {
					return entry.getValue();
				}
			}

			return (new ArrayList<String>(tMap.values())).get(0);

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "NoSuchAlgorithmException");
			e.printStackTrace();
			return null;
		}

	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		map.remove(selection);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public  Uri insert(Uri uri, ContentValues values) {

		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String coordinator = getCoordinator(key);
		String copy1 = null;
		String copy2 = null;
		switch(coordinator)
		{
			case "11108":
				copy1="11116";
				copy2="11120";
				break;
			case "11112":
				copy1="11108";
				copy2="11116";
				break;
			case "11116":
				copy1="11120";
				copy2="11124";
				break;
			case "11120":
				copy1="11124";
				copy2="11112";
				break;
			case "11124":
				copy1="11112";
				copy2="11108";
				break;
			default:
				Log.e(TAG,"Error in finding port no:"+myPort);
		}
		Log.i(TAG,"Inserting: "+key);

		Message message = new Message();
		message.setRemotePort(coordinator);
		message.setMsgType("Copy");
		message.setSenderPort(myPort);
		message.setKey(key);
		message.setValue(value);
		Log.i(TAG, "Inserting into coordinator");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

		Message message1 = new Message();
		message1.setRemotePort(copy1);
		message1.setMsgType("Copy");
		message1.setSenderPort(myPort);
		message1.setKey(key);
		message1.setValue(value);
		Log.i(TAG, "copying to Successor ports");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message1);

		Message message2 = new Message();
		message2.setRemotePort(copy2);
		message2.setMsgType("Copy");
		message2.setSenderPort(myPort);
		message2.setKey(key);
		message2.setValue(value);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message2);

		return null;
	}


	@Override
	public  Cursor query(Uri uri, String[] projection, String selection,
						 String[] selectionArgs, String sortOrder) {
		String key=selection;
		String[] cols = {"key", "value"};
		MatrixCursor cur = new MatrixCursor(cols);

		switch(key){
			case "*":
				Log.i(TAG, "Received * query");
				HashMap<String, String> result2 = new HashMap<>();
				for (Map.Entry<String, String> me : map.entrySet()) {
					result2.put(me.getKey(), me.getValue());
				}
				Message m = new Message();
				m.setResult(result2);
				m.setMsgType("AllQ");
				m.setSenderPort(myPort);
				m.setRemotePort(succ1);
				Log.i(TAG, "Forward the query request successorPort " + m.getRemotePort());
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m, null);

				Log.i(TAG, "Lock Inserted");
				synchronized (allQLock) {
					try {
						allQLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				Log.i(TAG, "Lock released");
				MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
				for (Map.Entry<String, String> entry : result.entrySet()) {
					matrixCursor.addRow(new Object[]{entry.getKey(), entry.getValue()});
				}
				cur = matrixCursor;
				return cur;


			case "@":
				Log.v("query", selection);
				for (Map.Entry<String, String> me : map.entrySet()) {
					cur.addRow(new String[]{me.getKey(), me.getValue()});
				}
				return cur;

			default:
				Log.i(TAG,"Got a query for: "+key);
				String coordinator = getCoordinator(key);
				if(coordinator.equals(myPort)){
					String value = map.get(selection);
					String[] b = {selection, value};
					cur.addRow(b);
					return cur;
				}
				else{
					// Forward to coordinator
					Log.i(TAG,"Forwarding to coordinator: "+coordinator);
					messageID++;
					Object lock = new Object();
					lockMap.put(messageID,lock);
					lock = lockMap.get(messageID);
					Message message = new Message();
					message.setSenderPort(myPort);
					message.setRemotePort(coordinator);
					message.setKey(key);
					message.setMsgType("SingleQ");
					message.setMessagID(messageID);

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
					synchronized (lock){
						try {
							Log.i(TAG,"Locking for single query reply. ID: "+messageID);
							lock.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					Log.i(TAG, "Received reply.");

					MatrixCursor matrixCursor1 = new MatrixCursor(new String[]{"key","value"});
					matrixCursor1.addRow(new Object[]{key,result.get(key)});
					cur = matrixCursor1;
					return cur;
				}

		}

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

}