package IMDBApplication;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class Populate {

    public void populateData() {
        try {
            deleteExistingRecords();
            populateMovies();
            populateActors();
            populateDirectors();
        } catch (Exception E) {
            System.err.print(E.getMessage());
        }
    }

    public void populateMovies() throws SQLException {
        Connection conn = null;
        PreparedStatement insertStmt = null;
        try {
            System.out.println("Populating Movies");
            conn = DBConnection.openConnection();
            String sqlQuey = "insert into MOVIES values" + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            insertStmt = conn.prepareStatement(sqlQuey);

            FileReader fileReader;
            BufferedReader bufferReader;
            String line;

            fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\movies.DAT");
            bufferReader = new BufferedReader(fileReader);
            bufferReader.readLine();
            while ((line = bufferReader.readLine()) != null) {

                String[] words = line.split("\\t");
                int id = Integer.parseInt(words[0]);
                String title = words[1];
                int imdbid = Integer.parseInt(words[2]);
                String sptitle = words[3];
                int year = Integer.parseInt(words[5]);

                float rtAllCriticRating = words[7].equals("\\N") ? 0.0f : Float.parseFloat(words[7]);
                float rtAllCriticsNumReviews = words[8].equals("\\N") ? 0.0f : Float.parseFloat(words[8]);
                float rtAllCriticsNumFresh = words[9].equals("\\N") ? 0.0f : Float.parseFloat(words[9]);
                float rtAllCriticsNumRotten = words[10].equals("\\N") ? 0.0f : Float.parseFloat(words[10]);
                float rtAllCriticsScore = words[11].equals("\\N") ? 0.0f : Float.parseFloat(words[11]);
                float rtTopCriticsRating = words[12].equals("\\N") ? 0.0f : Float.parseFloat(words[12]);
                float rtTopCriticsNumReviews = words[13].equals("\\N") ? 0.0f : Float.parseFloat(words[13]);
                float rtTopCriticsNumFresh = words[14].equals("\\N") ? 0.0f : Float.parseFloat(words[14]);
                float rtTopCriticsNumRotten = words[15].equals("\\N") ? 0.0f : Float.parseFloat(words[15]);
                float rtTopCriticsScore = words[16].equals("\\N") ? 0.0f : Float.parseFloat(words[16]);
                float rtAudienceRating = words[17].equals("\\N") ? 0.0f : Float.parseFloat(words[17]);
                float rtAudienceNumRatings = words[18].equals("\\N") ? 0.0f : Float.parseFloat(words[18]);
                float rtAudienceScore = words[19].equals("\\N") ? 0.0f : Float.parseFloat(words[19]);

                insertStmt.setInt(1, id);
                insertStmt.setString(2, title);
                insertStmt.setInt(3, imdbid);
                insertStmt.setString(4, sptitle);
                insertStmt.setInt(5, year);
                insertStmt.setFloat(6, rtAllCriticRating);
                insertStmt.setFloat(7, rtAllCriticsNumReviews);
                insertStmt.setFloat(8, rtAllCriticsNumFresh);
                insertStmt.setFloat(9, rtAllCriticsNumRotten);
                insertStmt.setFloat(10, rtAllCriticsScore);
                insertStmt.setFloat(11, rtTopCriticsRating);
                insertStmt.setFloat(12, rtTopCriticsNumReviews);
                insertStmt.setFloat(13, rtTopCriticsNumFresh);
                insertStmt.setFloat(14, rtTopCriticsNumRotten);
                insertStmt.setFloat(15, rtTopCriticsScore);
                insertStmt.setFloat(16, rtAudienceRating);
                insertStmt.setFloat(17, rtAudienceNumRatings);
                insertStmt.setFloat(18, rtAudienceScore);

                insertStmt.executeUpdate();
            }

            System.out.println("Populated Movies Successfully");
            
            populateGenres(conn);

            populateCountries(conn);

            populateLocations(conn);

            populateTags(conn);

            populateMovieTags(conn);

        } catch (Exception E) {
            System.err.print(E.getMessage());
        } finally {
            if (conn != null) {
                conn.close();
            }

            if (insertStmt != null) {
                insertStmt.close();
            }
        }
    }

    public void populateGenres(Connection conn) throws SQLException, IOException {
        PreparedStatement insertStmt = null;
        System.out.println("Populating Genres");
        String sqlQuey = "insert into GENRES values" + "(?,?)";
        insertStmt = conn.prepareStatement(sqlQuey);

        FileReader fileReader;
        BufferedReader bufferReader;
        String line;

        fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\movie_genres.DAT");
        bufferReader = new BufferedReader(fileReader);
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null) {

            String[] words = line.split("\\t");
            int id = Integer.parseInt(words[0]);
            String genre = words[1];

            insertStmt.setInt(1, id);
            insertStmt.setString(2, genre);
            insertStmt.executeUpdate();

        }
        
        System.out.println("Populated Genres Successfully");
    }

    public void populateCountries(Connection conn) throws SQLException, IOException {

        PreparedStatement insertStmt = null;
        System.out.println("Populating Countries");
        String sqlQuey = "insert into MOVIE_COUNTRIES values" + "(?,?)";
        insertStmt = conn.prepareStatement(sqlQuey);

        FileReader fileReader;
        BufferedReader bufferReader;
        String line;

        fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\movie_countries.DAT");
        bufferReader = new BufferedReader(fileReader);
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null) {

            String[] words = line.split("\\t");
            int id = Integer.parseInt(words[0]);
            String country = "";
            if (words.length == 1) {
                country = "NULL";
            } else {
                country = words[1];
            }

            insertStmt.setInt(1, id);
            insertStmt.setString(2, country);
            insertStmt.executeUpdate();
        }
        System.out.println("Populated Countries Successfully");

    }

    //Needs work
    public void populateLocations(Connection conn) throws SQLException, IOException {

        PreparedStatement insertStmt = null;
        System.out.println("Populating Locations");

        String sqlQuey = "insert into MOVIE_LOCATIONS values" + "(?,?,?,?,?)";
        insertStmt = conn.prepareStatement(sqlQuey);

        FileReader fileReader;
        BufferedReader bufferReader;
        String line;

        fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\movie_locations.DAT");
        bufferReader = new BufferedReader(fileReader);
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null) {

            String[] words = line.split("\\t");
            int id;
            String location1, location2, location3, location4;
            location1 = location2 = location3 = location4 = "";
            id = words.length > 0 ? Integer.parseInt(words[0]) : null;
            location1 = words.length > 1 ? words[1] : "NULL";
            location2 = words.length > 2 ? words[2] : "NULL";
            location3 = words.length > 3 ? words[3] : "NULL";
            location4 = words.length > 4 ? words[4] : "NULL";

            insertStmt.setInt(1, id);
            insertStmt.setString(2, location1);
            insertStmt.setString(3, location2);
            insertStmt.setString(4, location3);
            insertStmt.setString(5, location4);

            insertStmt.executeUpdate();
        }
        System.out.println("Populated Locations Successfully");


    }

    public void populateTags(Connection conn) throws SQLException, IOException {

        PreparedStatement insertStmt = null;
        System.out.println("Populating Tags");

        String sqlQuey = "insert into Tags values" + "(?,?)";
        insertStmt = conn.prepareStatement(sqlQuey);

        FileReader fileReader;
        BufferedReader bufferReader;
        String line;

        fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\tags.DAT");
        bufferReader = new BufferedReader(fileReader);
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null) {

            String[] words = line.split("\\t");
            int id = Integer.parseInt(words[0]);
            String value = words[1];

            insertStmt.setInt(1, id);
            insertStmt.setString(2, value);
            insertStmt.executeUpdate();
        }
        System.out.println("Populated Locations Successfully");

    }

    public void populateMovieTags(Connection conn) throws SQLException, IOException {

        PreparedStatement insertStmt = null;
        System.out.println("Populating Movie Tags");

        String sqlQuey = "insert into MOVIE_TAGS values" + "(?,?,?)";
        insertStmt = conn.prepareStatement(sqlQuey);

        FileReader fileReader;
        BufferedReader bufferReader;
        String line;

        fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\movie_tags.DAT");
        bufferReader = new BufferedReader(fileReader);
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null) {

            String[] words = line.split("\\t");
            int id = Integer.parseInt(words[0]);
            String tagID = words[1];
            String tagWeight = words[2];

            insertStmt.setInt(1, id);
            insertStmt.setString(2, tagID);
            insertStmt.setString(3, tagWeight);

            insertStmt.executeUpdate();
        }
        System.out.println("Populated Movie Tags Successfully");
    
}
    
    
    
    public void deleteExistingRecords()
    {
            deleteExistingData("GENRES");
            deleteExistingData("MOVIE_COUNTRIES");
            deleteExistingData("MOVIE_LOCATIONS");
            deleteExistingData("MOVIE_TAGS");
            deleteExistingData("Actors");
            deleteExistingData("DIRECTORS");
            deleteExistingData("Movies");
            deleteExistingData("TAGS");
            
        
    }
    
    public void deleteExistingData(String entity) {
        
        try{
            System.out.println("Deleting data from " + entity);
            Connection conn = DBConnection.openConnection();
            String sqlQuery="delete from "+entity;
            Statement st=conn.createStatement();
            st.execute(sqlQuery);
            System.out.println("Deletion Succesfull of "+ entity);
            conn.close();
            st.close();
        }
        catch(Exception E)
        {
            System.err.println(E.getMessage());
        }
        

    }
    
    
    
            
    
    public void populateActors() throws SQLException, IOException,ClassNotFoundException {
        PreparedStatement insertStmt = null;
        
        
        System.out.println("Populating Actors");
        
        String sqlQuery = "insert into Actors values" + "(?,?,?,?)";
        Connection conn=DBConnection.openConnection();
        insertStmt=conn.prepareStatement(sqlQuery);

        FileReader fileReader;
        BufferedReader bufferReader;
        String line;

        fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\movie_actors.DAT");
        bufferReader = new BufferedReader(fileReader);
        bufferReader.readLine();
        while ((line = bufferReader.readLine()) != null) {

            String[] words = line.split("\\t");
            int id = Integer.parseInt(words[0]);
            String actorID = words[1];
            String actorName= words[2];
            int ranking = Integer.parseInt(words[3]);

            insertStmt.setInt(1, id);
            insertStmt.setString(2, actorID);
            insertStmt.setString(3,actorName);
            insertStmt.setInt(4,ranking);
            insertStmt.executeUpdate();

        }
        
        System.out.println("Populated Actors Successfully");
        
        insertStmt.close();
        conn.close();
    }
    
     public void populateDirectors() throws SQLException, IOException,ClassNotFoundException {
        PreparedStatement insertStmt = null;
        
        
        System.out.println("Populating Directors");
        
        String sqlQuery = "insert into DIRECTORS values" + "(?,?,?)";
        Connection conn=DBConnection.openConnection();
        insertStmt=conn.prepareStatement(sqlQuery);

        FileReader fileReader;
        BufferedReader bufferReader;
        String line;

        fileReader = new FileReader("C:\\Users\\niran\\Desktop\\Nitya\\Quarter3\\DB\\Assignments\\HW3\\hetrec2011-movielens-2k-v2\\movie_directors.DAT");
        bufferReader = new BufferedReader(fileReader);
        bufferReader.readLine();
        
        while ((line = bufferReader.readLine()) != null) {

            String[] words = line.split("\\t");
            int id = Integer.parseInt(words[0]);
            String directorID = words[1];
            String directorName= words[2];

            insertStmt.setInt(1, id);
            insertStmt.setString(2, directorID);
            insertStmt.setString(3,directorName);
            insertStmt.executeUpdate();

        }
        
        System.out.println("Populated Directors Successfully");
        
        insertStmt.close();
        conn.close();
    }

public static void main(String[] args) {
        Populate popObj = new Populate();
        popObj.populateData();
    }
}
