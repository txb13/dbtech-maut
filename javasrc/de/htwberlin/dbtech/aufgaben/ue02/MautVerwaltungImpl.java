package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;

/**
 * Die Klasse realisiert die Mautverwaltung.
 * 
 * @author Patrick Dohmeier
 */
public class MautVerwaltungImpl implements IMautVerwaltung {

	private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {
		this.connection = connection;
	}



	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	@Override
	public String getStatusForOnBoardUnit(long fzg_id) {
		// TODO Auto-generated method stub

		PreparedStatement preparedstatement = null;
		ResultSet resultSet = null;
		String query = "SELECT STATUS FROM FAHRZEUGGERAT WHERE FZG_ID = ?";
		String res = "";
		try {

			preparedstatement = getConnection().prepareStatement(query);
			preparedstatement.setLong(1, fzg_id);
			resultSet = preparedstatement.executeQuery();
			if (resultSet.next()) {
				res = resultSet.getString("STATUS");
			}
		} catch (SQLException exp) {
			throw new RuntimeException(exp);
		} catch (NullPointerException exp) {
			throw new RuntimeException(exp);
		}
		return res;
	}

	@Override
	public int getUsernumber(int maut_id) {
		int nutzer_id = 0;

		String sql = "SELECT FZ.NUTZER_ID " +
				"FROM MAUTERHEBUNG ME " +
				"INNER JOIN FAHRZEUGGERAT FZG ON ME.FZG_ID = FZG.FZG_ID " +
				"INNER JOIN FAHRZEUG FZ ON FZG.FZ_ID = FZ.FZ_ID " +
				"WHERE ME.MAUT_ID = ?";

		try (PreparedStatement preparedStatement = getConnection().prepareStatement(sql)) {

			preparedStatement.setInt(1, maut_id);

			try (ResultSet resultSet = preparedStatement.executeQuery()) {

				if (resultSet.next()) {
					nutzer_id = resultSet.getInt("NUTZER_ID");
				}
			}
		} catch (SQLException e) {
			System.err.println("SQL-Fehler beim Abrufen der Nutzernummer: " + e.getMessage());
		}
		return nutzer_id;
	}

	@Override
	public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin, int achsen,
			int gewicht, String zulassungsland) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateStatusForOnBoardUnit(long fzg_id, String status) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteVehicle(long fz_id) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
		// TODO Auto-generated method stub
		return null;
	}

}
