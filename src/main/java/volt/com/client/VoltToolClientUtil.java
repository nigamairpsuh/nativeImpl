package volt.com.client;

import java.io.IOException;
import java.net.UnknownHostException;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class VoltToolClientUtil {

	static Client client;

	public static void main(String[] args) {
		
//		ClientResponse clientResponse = null;
//		ClientConfig config = new ClientConfig();
//		config.enableAutoTune();
//		Client client = ClientFactory.createClient(config);
//		try {
//			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//			System.out.println("Connecting to client");
//			client.createConnection("10.10.43.11:21213");
//			System.out.println("Connected");
//			System.out.println("Executing SP");
//			long startTime = System.currentTimeMillis();
//			double i = Math.random();
//			int qureyIndex = (int)(i*3);
//			if(qureyIndex==0)
//			clientResponse = client.callProcedure(ExecuteQuery.proc, 206877, 1914093, 20150629);
//			else if(qureyIndex==1)		
//			clientResponse = client.callProcedure(ExecuteQuery.proc, 110324, 1913978, 20150721);
//			else  
//			clientResponse = client.callProcedure(ExecuteQuery.proc, 206877, 1914093, 20150629);
//			
//			VoltTable[] voltdbResultSet = clientResponse.getResults();
//			long endTime = System.currentTimeMillis();
//			System.out.println("Time taken to execute SP " + (endTime - startTime));
//			System.out.println("VoltResult size " + voltdbResultSet.length);
//			if (voltdbResultSet != null && voltdbResultSet.length > 0) {
//
//				VoltTable voltTable = voltdbResultSet[0];
//				voltTable.advanceRow();
//				System.out.print("AdvertiserId: " +  voltTable.get(0, VoltType.INTEGER));
//				System.out.print(" CampaignId: " +  voltTable.get(1, VoltType.INTEGER));
//				System.out.print(" BalanceDate: " +  voltTable.get(2, VoltType.INTEGER));
//				System.out.print(" CampaignRunningDailySpend " +  voltTable.getDecimalAsBigDecimal(3));
//				System.out.print(" CampaignRunningDailyPushSpend " +  voltTable.getDecimalAsBigDecimal(4));
//				System.out.print(" AdvertiserMaxDailyBudget " +  voltTable.getDecimalAsBigDecimal(5));
//				System.out.print(" CampaignMaxDailyBudget " +  voltTable.getDecimalAsBigDecimal(6));
//				System.out.print(" TotalCountDailyPush " +  voltTable.get(7, VoltType.INTEGER));
//				System.out.print(" TotalCountDailyClick " +  voltTable.get(8, VoltType.INTEGER));
//				System.out.print(" AdvertiserRunningDailySpend " +  voltTable.getDecimalAsBigDecimal(9));
//				System.out.print(" AdvertiserRunningDailyPushSpend " +  voltTable.getDecimalAsBigDecimal(10));
//				System.out.print(" AccountLedgerCredits " +  voltTable.getDecimalAsBigDecimal(11));
//				System.out.print(" AccountLedgerDebits " +  voltTable.getDecimalAsBigDecimal(12));
//				System.out.print(" AdvertiserAccountBalance " +  voltTable.getDecimalAsBigDecimal(13));
//			}
//
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ProcCallException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

	static Client getClient() {
//		if (client != null)
//			return client;

		ClientConfig config = new ClientConfig();
		config.enableAutoTune();
		Client client = ClientFactory.createClient(config);
		try {
			client.createConnection("10.10.43.11:21213");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return client;
	}

}
