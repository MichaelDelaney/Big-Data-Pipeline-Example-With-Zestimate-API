package com.bigdataprocessing;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Set;
import static jdk.nashorn.internal.objects.NativeString.indexOf;

/**
 * Persistence service to take a record, parse it, and persist it
 * into a cassandra database.
 */
public class Persistence {


    public static void persistRecord(ConsumerRecord<String, String> record){

        String contents = record.toString();

        // Retrieves Zillow Property ID
        int zpidIndex = indexOf(contents, "<zpid>");
        int zpidEndIndex = indexOf(contents, "</zpid>");

        // Retrieves Property Address
        int streetIndex = indexOf(contents, "<street>");
        int streetEndIndex = indexOf(contents, "</street>");
        int cityIndex = indexOf(contents, "<city>");
        int cityEndIndex = indexOf(contents, "</city>");
        int stateIndex = indexOf(contents, "<state>");
        int stateEndIndex = indexOf(contents, "</state>");
        int zipcodeIndex = indexOf(contents, "<zipcode>");
        int zipcodeEndIndex = indexOf(contents, "</zipcode>");

        // Retrieves price of property
        int priceIndex = indexOf(contents, "<amount");
        int priceEndIndex = indexOf(contents, "</amount>");

        // Retrieves link that has home details
        int homeDetailsIntex = indexOf(contents, "<homedetails>");
        int homeDetailsEndIndex = indexOf(contents, "</homedetails>");

        Set<Integer> indexSet = Sets.newHashSet(zpidIndex, zpidEndIndex, streetIndex, streetEndIndex,
                zipcodeIndex, zipcodeEndIndex, priceIndex, priceEndIndex, cityIndex, cityEndIndex,
                stateIndex, stateEndIndex, homeDetailsIntex, homeDetailsEndIndex);

        if(!indexSet.contains(-1)) {

            // Gathers values to pass into cql query
            int zpid = Integer.parseInt(contents.substring(zpidIndex + 6, zpidEndIndex));
            String street = contents.substring(streetIndex + 8, streetEndIndex);
            String city = contents.substring(cityIndex + 6, cityEndIndex);
            String state = contents.substring(stateIndex + 7, stateEndIndex);
            String zipcode = contents.substring(zipcodeIndex + 9, zipcodeEndIndex);
            int price = Integer.parseInt(contents.substring(priceIndex + 23, priceEndIndex));
            String homeDetails = contents.substring(homeDetailsIntex + 13, homeDetailsEndIndex);

            // Builds query
            String query = "INSERT INTO property (zpid, street,city, state, zipcode, price, home_details)"
                    + " VALUES(" + zpid + ", '"+street+"', '"+city+"', '"+state+
                    "', '"+zipcode+"', "+price+", '"+homeDetails+"' );";

            // Executes query
            System.out.println(query);
            Cluster cluster = Cluster.builder()
                    .withPort(9042)
                    .addContactPoint("localhost").build();
            Session session = cluster.connect("zillow_app");
            session.execute(query);
            System.out.println("Successfully persisted new record.");
        }
    }
}
