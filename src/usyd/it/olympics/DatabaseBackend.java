package usyd.it.olympics;


/**
 * Database back-end class for simple gui.
 *
 * The DatabaseBackend class defined in this file holds all the methods to
 * communicate with the database and pass the results back to the GUI.
 *
 *
 * Make sure you update the dbname variable to your own database name. You
 * can run this class on its own for testing without requiring the GUI.
 */
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Database interfacing backend for client. This class uses JDBC to connect to
 * the database, and provides methods to obtain query data.
 *
 * Most methods return database information in the form of HashMaps (sets of
 * key-value pairs), or ArrayLists of HashMaps for multiple results.
 *
 * @author Bryn Jeffries {@literal <bryn.jeffries@sydney.edu.au>}
 */
public class DatabaseBackend {

    ///////////////////////////////
    /// DB Connection details
    /// These are set in the constructor so you should never need to read or
    /// write to them yourself
    ///////////////////////////////
    private final String dbUser;
    private final String dbPass;
    private final String connstring;


    ///////////////////////////////
    /// Student Defined Functions
    ///////////////////////////////

    /////  Login and Member  //////

    /**
     * Validate memberID details
     *
     * Implements Core Functionality (a)
     *
     * @return basic details of user if username is for a valid memberID and password is correct
     * @throws OlympicsDBException
     * @throws SQLException
     */
    public HashMap<String,Object> checkLogin(String member, char[] password) throws OlympicsDBException  {
        // Note that password is a char array for security reasons.
        // Don't worry about this: just turn it into a string to use in this function
        // with "new String(password)"
        Connection conn = null;
        HashMap<String,Object> details = null;
        if (member.isEmpty() || !member.matches("^[0-9A-Za-z]+$")) {
            return details;
        }
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT member_id, title, given_names, family_name, country_name, place_name, athlete_id, pass_word, official_id " +
                    "FROM member JOIN country USING (country_code) " +
                    "JOIN Accommodation ON (accommodation = place_id) " +
                    "JOIN Place USING (place_id) " +
                    "LEFT OUTER JOIN (SELECT member_id AS Athlete_id FROM Athlete) foo ON (member_id = Athlete_id) " +
                    "LEFT OUTER JOIN (SELECT member_id AS official_id FROM Official) foo2 ON (member_id = official_id)" +
                    "WHERE member_id = ?");
            stmt.setString(1, member);
            ResultSet rs = stmt.executeQuery();
            conn.close();

            String member_id = null;
            String title = null;
            String first_name = null;
            String family_name = null;
            String country_name = null;
            String residence = null;
            String athlete_id = null;
            String official_id = null;
            String pass_word = null;
            String member_type = null;

            if (rs.next()) {
                member_id = rs.getString("member_id");
                title = rs.getString("title");
                first_name = rs.getString("given_names");
                family_name = rs.getString("family_name");
                country_name = rs.getString("country_name");
                residence = rs.getString("place_name");
                athlete_id = rs.getString("athlete_id");
                official_id = rs.getString("official_id");
                pass_word = rs.getString("pass_word");
            }

            if (athlete_id != null) member_type = "athlete";
            else if (official_id != null) member_type = "official";
            else member_type = "staff";

            // Don't forget you have memberID variables memberUser available to
            // use in a query.
            // Query whether login (memberID, password) is correct...
            boolean valid = (member.equals(member_id) && new String(password).equals(pass_word));
            if (valid) {
                details = new HashMap<String,Object>();

                // Populate with record data
                details.put("member_id", member);
                details.put("title", title);
                details.put("first_name", first_name);
                details.put("family_name", family_name);
                details.put("country_name", country_name);
                details.put("residence", residence);
                details.put("member_type", member_type);            }


        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Error checking login details", e);
        }  finally {
            reallyClose(conn);
        }
        return details;
    }

    /**
     * Obtain details for the current memberID
     * @param memberID
     *
     * @return Details of member
     * @throws OlympicsDBException
     */
    public HashMap<String, Object> getMemberDetails(String memberID) throws OlympicsDBException {

        HashMap<String, Object> details = new HashMap<String, Object>();
        Connection conn = null;
        if (memberID.isEmpty() || !memberID.matches("^[0-9A-Za-z]+$")) {
            return details;
        }

        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT member_id, title, given_names, family_name, country_name, place_name, athlete_id, official_id, COUNT(journey_id) AS num_bookings " +
                    "FROM member JOIN country USING (country_code) " +
                    "JOIN Accommodation ON (accommodation = place_id) " +
                    "JOIN Place USING (place_id) " +
                    "LEFT OUTER JOIN (SELECT member_id AS Athlete_id FROM Athlete) foo ON (member_id = Athlete_id) " +
                    "LEFT OUTER JOIN (SELECT member_id AS official_id FROM Official) foo2 ON (member_id = official_id) " +
                    "LEFT OUTER JOIN Booking ON (member_id = booked_for) " +
                    "WHERE member_id = ?" +
                    "GROUP BY member_id, title, given_names, family_name, country_name, place_name, athlete_id, pass_word, official_id");
            stmt.setString(1, memberID);
            ResultSet rs = stmt.executeQuery();
            conn.close();


            if (rs.next()) {
                String title = rs.getString("title");
                String given_name = rs.getString("given_names");
                String family_name = rs.getString("family_name");
                String country_name = rs.getString("country_name");
                String residence = rs.getString("place_name");
                int numbookings = rs.getInt("num_bookings");
                String athlete_id = rs.getString("athlete_id");
                String official_id = rs.getString("official_id");
                String member_type;

                if (athlete_id != null) member_type = "athlete";
                else if (official_id != null) member_type = "official";
                else member_type = "staff";

                details.put("member_id", memberID);
                details.put("member_type", member_type);
                details.put("title", title);
                details.put("first_name", given_name);
                details.put("family_name", family_name);
                details.put("country_name", country_name);
                details.put("residence", residence);
                details.put("num_bookings", numbookings);

                if (member_type.equals("athlete")) {

                    //-- gold
                    conn = getConnection();
                    stmt = conn.prepareStatement("SELECT COUNT(*) AS gold FROM Participates "+
                            "WHERE athlete_id = ? AND medal = ?");
                    stmt.setString(1, memberID);
                    stmt.setString(2, "G");
                    rs = stmt.executeQuery();
                    conn.close();

                    int num_gold = 0;
                    if (rs.next()) {
                        num_gold += rs.getInt("gold");
                    }
                    // team gold
                    conn = getConnection();
                    stmt = conn.prepareStatement(
                            "SELECT COUNT(*) AS TeamGold " +
                                    "FROM TeamMember JOIN Team USING (team_name, event_id) " +
                                    "WHERE athlete_id = ? AND medal = ? ");
                    stmt.setString(1, memberID);
                    stmt.setString(2, "G");

                    rs = stmt.executeQuery();
                    conn.close();
                    if (rs.next()) {
                        num_gold += rs.getInt("TeamGold");
                    }
                    details.put("num_gold", num_gold);

                    // silver

                    conn = getConnection();
                    stmt = conn.prepareStatement("SELECT COUNT(*) AS silver FROM Participates "+
                            "WHERE athlete_id = ? AND medal = ?");
                    stmt.setString(1, memberID);
                    stmt.setString(2, "S");
                    rs = stmt.executeQuery();
                    conn.close();

                    int num_silver = 0;
                    if (rs.next()) {
                        num_silver = rs.getInt("silver");
                    }

                    // team silver

                    conn = getConnection();
                    stmt = conn.prepareStatement(
                            "SELECT COUNT(*) AS TeamSilver " +
                                    "FROM TeamMember JOIN Team USING (team_name, event_id) " +
                                    "WHERE athlete_id = ? AND medal = ?");
                    stmt.setString(1, memberID);
                    stmt.setString(2, "S");
                    rs = stmt.executeQuery();
                    conn.close();

                    if (rs.next()) {
                        num_silver += rs.getInt("TeamSilver");
                    }
                    details.put("num_silver", num_silver);

                    // bronze

                    conn = getConnection();
                    stmt = conn.prepareStatement("SELECT COUNT(*) AS bronze FROM Participates "+
                            "WHERE athlete_id = ? AND medal = ?");
                    stmt.setString(1, memberID);
                    stmt.setString(2, "B");
                    rs = stmt.executeQuery();
                    conn.close();

                    int num_bronze = 0;
                    if (rs.next()) {
                        num_bronze = rs.getInt("bronze");
                    }

                    // team bronze

                    conn = getConnection();
                    stmt = conn.prepareStatement(
                            "SELECT COUNT(*) AS TeamBronze " +
                                    "FROM TeamMember JOIN Team USING (team_name, event_id) " +
                                    "WHERE athlete_id = ? AND medal = ?");
                    stmt.setString(1, memberID);
                    stmt.setString(2, "B");
                    rs = stmt.executeQuery();
                    conn.close();

                    if (rs.next()) {
                        num_bronze += rs.getInt("TeamBronze");
                    }
                    details.put("num_bronze", num_bronze);
                }
            }


        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Displaying greeting message error", e);
        } finally {
            reallyClose(conn);
        }
        return details;
    }


    //////////  Events  //////////

    /**
     * Get all of the events listed in the olympics for a given sport
     *
     * @param sportId the ID of the sport we are filtering by
     * @return List of the events for that sport
     * @throws OlympicsDBException
     */
    ArrayList<HashMap<String, Object>> getEventsOfSport(Integer sportId) throws OlympicsDBException {

        ArrayList<HashMap<String, Object>> events = new ArrayList<>();
        Connection conn = null;
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT event_id, sport_id, event_name, event_start, event_gender, place_name " +
                            "FROM Event JOIN (SportVenue NATURAL JOIN Place) foo ON (sport_venue = place_id)" +
                            "WHERE sport_id = ?");
            stmt.setInt(1, sportId);

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                int event_id = rs.getInt("event_id");
                int sport_id = rs.getInt("sport_id");
                String event_name = rs.getString("event_name");
                Date event_start = new Date(rs.getTimestamp("event_start").getTime());
                String event_gender = rs.getString("event_gender");
                String sport_venue = rs.getString("place_name");

                HashMap<String,Object> event = new HashMap<String,Object>();
                event.put("event_id", event_id);
                event.put("sport_id", sport_id);
                event.put("event_name", event_name);
                event.put("event_gender", event_gender);
                event.put("sport_venue", sport_venue);
                event.put("event_start", event_start);
                events.add(event);
            }

            conn.close();
        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Acquiring events error", e);
        } finally {
            reallyClose(conn);
        }

        return events;
    }

    /**
     * Retrieve the results for a single event
     * @param eventId the key of the event
     * @return a hashmap for each result in the event.
     * @throws OlympicsDBException
     */
    @SuppressWarnings("resource")
	ArrayList<HashMap<String, Object>> getResultsOfEvent(Integer eventId) throws OlympicsDBException {

        ArrayList<HashMap<String, Object>> results = new ArrayList<>();
        Connection conn = null;
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM Event JOIN Team USING(event_id) WHERE event_id = ?");
            stmt.setInt(1, eventId);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            conn.close();
            int i = rs.getInt("count");
            if (i == 0) {
                conn = getConnection();
                stmt = conn.prepareStatement(
                        "SELECT ( family_name || ', ' || given_names) AS name , country_name, medal " +
                        "FROM Event NATURAL JOIN Participates JOIN Member ON (athlete_id = member_id) JOIN Country USING (country_code)" +
                        "WHERE event_id = ? ORDER BY name");
                stmt.setInt(1, eventId);
                rs = stmt.executeQuery();
                conn.close();

                while (rs.next()) {
                    String name = rs.getString("name");
                    String country_name = rs.getString("country_name");
                    String medal_code = rs.getString("medal");
                    String medal = null;
                    if (medal_code != null) {
                        if (medal_code.equals("G")) medal = "Gold";
                        else if (medal_code.equals("S")) medal = "Silver";
                        else if (medal_code.equals("B")) medal = "Bronze";
                    }

                    HashMap<String, Object> result = new HashMap<String, Object>();
                    result.put("participant", name);
                    result.put("country_name", country_name);
                    result.put("medal", medal);
                    results.add(result);
                }

            } else {
                conn = getConnection();

                stmt = conn.prepareStatement("SELECT team_name, country_name, medal " +
                        "FROM Event JOIN Team USING (event_id) JOIN Country USING (country_code)" +
                        "WHERE event_id = ? ORDER BY team_name");
                stmt.setInt(1, eventId);
                rs = stmt.executeQuery();
                conn.close();

                while (rs.next()) {
                    String name = rs.getString("team_name");
                    String country_name = rs.getString("country_name");
                    String medal_code = rs.getString("medal");
                    String medal = null;
                    if (medal_code != null) {
                        if (medal_code.equals("G")) medal = "Gold";
                        else if (medal_code.equals("S")) medal = "Silver";
                        else if (medal_code.equals("B")) medal = "Bronze";
                    }

                    HashMap<String, Object> result = new HashMap<>();
                    result.put("participant", name);
                    result.put("country_name", country_name);
                    result.put("medal", medal);
                    results.add(result);
                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new OlympicsDBException("Acquiring results of event error", e);
        } finally {
            reallyClose(conn);
        }

        return results;
    }



    ///////   Journeys    ////////

    /**
     * Array list of journeys from one place to another on a given date
     * @param journeyDate the date of the journey
     * @param fromPlace the origin, starting place.
     * @param toPlace the destination, place to go to.
     * @return a list of all journeys from the origin to destination
     */
    ArrayList<HashMap<String, Object>> findJourneys(String fromPlace, String toPlace, Date journeyDate) throws OlympicsDBException {

        ArrayList<HashMap<String, Object>> journeys = new ArrayList<>();
        Connection conn = null;
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT journey_id, vehicle_code, origin_name, dest_name, depart_time, arrive_time, nbooked, capacity " +
                            "FROM Journey JOIN Vehicle USING (vehicle_code) " +
                            "JOIN (SELECT place_name AS origin_name, place_id AS from_place FROM place) AS origin USING (from_place) " +
                            "JOIN (SELECT place_name AS dest_name, place_id AS to_place FROM place) AS dest USING (to_place) " +
                            "WHERE origin_name = ? " +
                            "AND dest_name = ? " +
                            "AND DATE_TRUNC('day', depart_time) = ?");
            stmt.setString(1, fromPlace);
            stmt.setString(2, toPlace);
            stmt.setDate(3, new java.sql.Date(journeyDate.getTime()));

            ResultSet rs = stmt.executeQuery();
            conn.close();

            while (rs.next()) {

                int journey_id = rs.getInt("journey_id");
                String vehicle_code = rs.getString("vehicle_code");
                String origin_name = rs.getString("origin_name");
                String dest_name = rs.getString("dest_name");
                Date depart_time = new Date(rs.getTimestamp("depart_time").getTime());
                Date arrive_time = new Date(rs.getTimestamp("arrive_time").getTime());
                int nbooked = rs.getInt("nbooked");
                int capacity = rs.getInt("capacity");
                int available_seats = capacity - nbooked;

                HashMap<String, Object> journey = new HashMap<>();
                journey.put("journey_id", journey_id);
                journey.put("vehicle_code", vehicle_code);
                journey.put("origin_name", origin_name);
                journey.put("dest_name", dest_name);
                journey.put("when_departs", depart_time);
                journey.put("when_arrives", arrive_time);
                journey.put("available_seats", available_seats);
                journeys.add(journey);
            }

        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Acquiring journey info error", e);
        } finally {
            reallyClose(conn);
        }
        return journeys;
    }

    ArrayList<HashMap<String,Object>> getMemberBookings(String memberID) throws OlympicsDBException {

        ArrayList<HashMap<String,Object>> bookings = new ArrayList<HashMap<String,Object>>();
        Connection conn = null;
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT journey_id, vehicle_code, origin_name, dest_name, depart_time, arrive_time " +
                            "From Booking JOIN Journey USING (journey_id) JOIN (SELECT place_name AS origin_name, place_id AS from_place FROM place) AS origin USING (from_place) " +
                            "JOIN (SELECT place_name AS dest_name, place_id AS to_place FROM place) AS dest USING (to_place) " +
                            "WHERE booked_for = ?");
            stmt.setString(1, memberID);
            ResultSet rs = stmt.executeQuery();
            conn.close();

            while (rs.next()) {
                int journey_id = rs.getInt("journey_id");
                String vehicle_code = rs.getString("vehicle_code");
                String origin_name = rs.getString("origin_name");
                String dest_name = rs.getString("dest_name");
                Timestamp when_departs = rs.getTimestamp("depart_time");
                Timestamp when_arrives = rs.getTimestamp("arrive_time");

                HashMap<String, Object> bookingex = new HashMap<>();
                bookingex.put("journey_id", journey_id);
                bookingex.put("vehicle_code", vehicle_code);
                bookingex.put("origin_name", origin_name);
                bookingex.put("dest_name", dest_name);
                bookingex.put("when_departs", when_departs);
                bookingex.put("when_arrives", when_arrives);
                bookings.add(bookingex);
            }
            conn.close();
        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Acquiring Member Bookings error", e);
        } finally {
            reallyClose(conn);
        }
        return bookings;
    }

    /**
     * Get details for a specific journey
     *
     * @return Various details of journey - see JourneyDetails.java
     * @throws OlympicsDBException
     * @param journeyId
     */
    public HashMap<String,Object> getJourneyDetails(Integer journeyId) throws OlympicsDBException {

        HashMap<String,Object> details = new HashMap<String,Object>();
        Connection conn = null;

        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT journey_id, vehicle_code, origin_name, dest_name, depart_time, arrive_time, nbooked, capacity " +
                            "FROM Journey JOIN Vehicle USING (vehicle_code) " +
                            "JOIN (SELECT place_name AS origin_name, place_id AS from_place FROM place) AS origin USING (from_place) " +
                            "JOIN (SELECT place_name AS dest_name, place_id AS to_place FROM place) AS dest USING (to_place) " +
                            "WHERE journey_id = ?");
            stmt.setInt(1, journeyId);

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                int journey_id = rs.getInt("journey_id");
                String vehicle_code = rs.getString("vehicle_code");
                String origin_name = rs.getString("origin_name");
                String dest_name = rs.getString("dest_name");
                Date depart_time = new Date(rs.getTimestamp("depart_time").getTime());
                Date arrive_time = new Date(rs.getTimestamp("arrive_time").getTime());
                int capacity = rs.getInt("capacity");
                int nbooked = rs.getInt("nbooked");

                details.put("journey_id", journey_id);
                details.put("vehicle_code", vehicle_code);
                details.put("origin_name", origin_name);
                details.put("dest_name", dest_name);
                details.put("when_departs", depart_time);
                details.put("when_arrives", arrive_time);
                details.put("capacity", capacity);
                details.put("nbooked", nbooked);
            }
            conn.close();
        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Acquiring Journey details error", e);
        } finally {
            reallyClose(conn);
        }

        return details;
    }

    @SuppressWarnings("resource")
	public HashMap<String,Object> makeBooking(String byStaff, String forMember, String vehicle, Date departs) throws OlympicsDBException {
        HashMap<String,Object> booking = null;

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            
            stmt = conn.prepareStatement(
            		  "SELECT nbooked, capacity, journey_id "
            		+ "FROM Journey NATURAL JOIN Vehicle "
            		+ "WHERE vehicle_code = ? AND depart_time = ?");
            stmt.setString(1, vehicle);
            stmt.setTimestamp(2, new Timestamp(departs.getTime()));
            
            ResultSet rs = stmt.executeQuery();
            rs.next();
            
            int nbooked = rs.getInt("nbooked");
            int capacity = rs.getInt("capacity");
            int journey_id = rs.getInt("journey_id");
            if (nbooked >= capacity) {
                conn.setAutoCommit(true);
                return booking;
            }
            
            //SimpleDateFormat day = new SimpleDateFormat("dd/MM/yyyy");
            
            stmt = conn.prepareStatement(
                    		"SELECT to_name, from_name " +
                            "FROM Journey JOIN (SELECT place_id AS to_id, place_name AS to_name FROM Place) AS foo ON (to_place = to_id) " +
                            "JOIN (SELECT place_id AS from_id, place_name AS from_name FROM Place) AS foo2 ON (from_place = from_id) " +
                            "WHERE journey_id = ?"
            );
            
            stmt.setInt(1, journey_id);
            rs = stmt.executeQuery();
            rs.next();
            String to_name = rs.getString("to_name");
            String from_name = rs.getString("from_name");

            stmt = conn.prepareStatement(
                    		"SELECT member_id " +
                            "FROM Member WHERE (family_name||', '|| given_names) = ?"
            );
            stmt.setString(1, forMember);
            rs = stmt.executeQuery();
            rs.next();
            String booked_for = rs.getString("member_id");

            stmt = conn.prepareStatement(
                    		"SELECT (family_name||', '||given_names) AS bookedby_name " +
                            "FROM Member WHERE member_id = ?"
            );
            stmt.setString(1, byStaff);
            rs = stmt.executeQuery();
            rs.next();
            String bookedby_name = rs.getString("bookedby_name");

            stmt = conn.prepareStatement(
                    		"SELECT arrive_time, depart_time FROM Journey WHERE journey_id = ?"
            );
            stmt.setInt(1, journey_id);
            rs = stmt.executeQuery();
            rs.next();
            Date when_arrives = new Date(rs.getTimestamp("arrive_time").getTime());
            Date when_departs = new Date(rs.getTimestamp("depart_time").getTime());

            stmt = conn.prepareStatement("INSERT INTO Booking VALUES (?, ?, ?, ?)");
            stmt.setString(1, booked_for);
            stmt.setString(2, byStaff);
            stmt.setTimestamp(3, new Timestamp(new Date().getTime()));
            stmt.setInt(4, journey_id);
            stmt.executeUpdate();

            stmt = conn.prepareStatement("UPDATE Journey SET nbooked = nbooked + 1 WHERE journey_id = ?");
            stmt.setInt(1, journey_id);
            stmt.executeUpdate();

            conn.commit();
            conn.setAutoCommit(true);
            conn.close();

            booking = new HashMap<>();
            booking.put("vehicle_code", vehicle); // 
            booking.put("when_departs", when_departs); // getTimestamp("arrive_time").getTime()
            booking.put("when_arrives", when_arrives); //  getTimestamp("arrive_time").getTime()
            booking.put("dest_name", to_name); //
            booking.put("origin_name", from_name); //
            booking.put("bookedby_name", bookedby_name); // staff_name
            booking.put("bookedfor_name", forMember); // mem_name
            booking.put("when_booked", new Timestamp(new Date().getTime())); // current time  -- format getTimestamp("arrive_time").getTime() // when booked


        } catch (SQLException se) {
            se.printStackTrace();
            try {
                if (conn != null) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException se3) {
                se3.printStackTrace();
            }
            reallyClose(conn);
        }
        return booking;
    }


    public HashMap<String,Object> getBookingDetails(String memberID, Integer journeyId) throws OlympicsDBException {

        HashMap<String,Object> booking = new HashMap<String,Object>();
        Connection conn = null;
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT bookedfor_name, bookedby_name, when_booked, journey_id, vehicle_code, origin_name, dest_name, depart_time, arrive_time " +
                            "FROM Booking JOIN Journey USING (journey_id) " +
                            "JOIN (SELECT (family_name || \', \' || given_names) AS bookedfor_name, member_id AS bookedfor_id FROM Member) AS bookedfor ON (booked_for = bookedfor_id)" +
                            "JOIN (SELECT (family_name || \', \' || given_names) AS bookedby_name, member_id AS bookedby_id FROM Member) AS bookedby ON (booked_by = bookedby_id)" +
                            "JOIN (SELECT place_name AS origin_name, place_id AS from_place FROM place) AS origin USING (from_place) " +
                            "JOIN (SELECT place_name AS dest_name, place_id AS to_place FROM place) AS dest USING (to_place) " +
                            "WHERE booked_for = ? AND journey_id = ?");
            stmt.setString(1, memberID);
            stmt.setInt(2, journeyId);
            ResultSet rs = stmt.executeQuery();
            conn.close();

            while (rs.next()) {
                String bookedby_name = rs.getString("bookedby_name");
                String bookedfor_name = rs.getString("bookedfor_name");
                Timestamp when_booked = rs.getTimestamp("when_booked");
                int journey_id = rs.getInt("journey_id");
                String vehicle_code = rs.getString("vehicle_code");
                String origin_name = rs.getString("origin_name");
                String dest_name = rs.getString("dest_name");
                Timestamp depart_time = rs.getTimestamp("depart_time");
                Timestamp arrive_time = rs.getTimestamp("arrive_time");

                booking.put("bookedby_name", bookedby_name);
                booking.put("bookedfor_name", bookedfor_name);
                booking.put("when_booked", when_booked);
                booking.put("journey_id", journey_id);
                booking.put("vehicle", vehicle_code);
                booking.put("origin_name", origin_name);
                booking.put("dest_name", dest_name);
                booking.put("when_departs", depart_time);
                booking.put("when_arrives", arrive_time);
            }
        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Acquiring Booking Details error", e);
        } finally {
            reallyClose(conn);
        }
        return booking;
    }

    public ArrayList<HashMap<String, Object>> getSports() throws OlympicsDBException {

        ArrayList<HashMap<String,Object>> sports = new ArrayList<HashMap<String,Object>>();
        Connection conn = null;
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement("SELECT sport_id, sport_name, discipline FROM sport");
            ResultSet rs = stmt.executeQuery();
            conn.close();

            while (rs.next()) {
                int sport_id = rs.getInt("sport_id");
                String sport_name = rs.getString("sport_name");
                String discipline = rs.getString("discipline");

                HashMap<String, Object> sport = new HashMap<>();
                sport.put("sport_id", sport_id);
                sport.put("sport_name", sport_name);
                sport.put("discipline", discipline);
                sports.add(sport);
            }
            conn.close();
        } catch (SQLException e) {
            throw new OlympicsDBException(e.getMessage(), e);
        } catch (Exception e) {
            throw new OlympicsDBException("Acquiring Sports Details error", e);
        } finally {
            reallyClose(conn);
        }
        return sports;
    }


    /////////////////////////////////////////
    /// Functions below don't need
    /// to be touched.
    ///
    /// They are for connecting and handling errors!!
    /////////////////////////////////////////

    /**
     * Default constructor that simply loads the JDBC driver and sets to the
     * connection details.
     *
     * @throws ClassNotFoundException if the specified JDBC driver can't be
     * found.
     * @throws OlympicsDBException anything else
     */
    DatabaseBackend(InputStream config) throws ClassNotFoundException, OlympicsDBException {
        Properties props = new Properties();
        try {
            props.load(config);
        } catch (IOException e) {
            throw new OlympicsDBException("Couldn't read config data",e);
        }

        dbUser = props.getProperty("username");
        dbPass = props.getProperty("userpass");
        String port = props.getProperty("port");
        String dbname = props.getProperty("dbname");
        String server = props.getProperty("address");;

        // Load JDBC driver and setup connection details
        String vendor = props.getProperty("dbvendor");
        if(vendor==null) {
            throw new OlympicsDBException("No vendor config data");
        } else if ("postgresql".equals(vendor)) {
            Class.forName("org.postgresql.Driver");
            connstring = "jdbc:postgresql://" + server + ":" + port + "/" + dbname;
        } else if ("oracle".equals(vendor)) {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connstring = "jdbc:oracle:thin:@" + server + ":" + port + ":" + dbname;
        } else throw new OlympicsDBException("Unknown database vendor: " + vendor);

        // test the connection
        Connection conn = null;
        try {
            conn = getConnection();
        } catch (SQLException e) {
            throw new OlympicsDBException("Couldn't open connection", e);
        } finally {
            reallyClose(conn);
        }
    }

    /**
     * Utility method to ensure a connection is closed without
     * generating any exceptions
     * @param conn Database connection
     */
    private void reallyClose(Connection conn) {
        if(conn!=null)
            try {
                conn.close();
            } catch (SQLException ignored) {}
    }

    /**
     * Construct object with open connection using configured login details
     * @return database connection
     * @throws SQLException if a DB connection cannot be established
     */
    private Connection getConnection() throws SQLException {
        Connection conn;
        conn = DriverManager.getConnection(connstring, dbUser, dbPass);
        return conn;
    }




}