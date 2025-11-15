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
		final String sql = "INSERT INTO FAHRZEUG " + "(FZ_ID, SSKL_ID, NUTZER_ID, KENNZEICHEN, FIN, ACHSEN, GEWICHT, ZULASSUNGSLAND)" + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSDATE)";
        try(PreparedStatement ps = getConnection().prepareStatement(sql)){
            ps.setLong(1,fz_id);
            ps.setInt(2, sskl_id);
            ps.setInt(3, nutzer_id);
            ps.setString(4, kennzeichen);
            ps.setString(5, fin);
            ps.setInt(6, achsen);
            ps.setInt(7, gewicht);
            ps.setString(8, zulassungsland);
            int affected = ps.executeUpdate();
            if (affected != 1){
                throw new RuntimeException("registerVehicle: insert affected " + affected + " rows (expected 1)");
            }
            L.info("Fahrzeug {} ({}) registriert.", fz_id,kennzeichen);
        }catch (SQLException e){
            throw new RuntimeException("registerVehicle failed for fz_id=" + fz_id,e);
        }

	}

	@Override
	public void updateStatusForOnBoardUnit(long fzg_id, String status) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteVehicle(long fz_id) {
		final String deleteMaut =
				"DELETE FROM MAUTERHEBUNG " +
						"WHERE FZG_ID IN (SELECT FZG_ID FROM FAHRZEUGGERAT WHERE FZ_ID = ?)";
		final String deleteGeraet =
				"DELETE FROM FAHRZEUGGERAT WHERE FZ_ID = ?";
		final String deleteFzg =
				"DELETE FROM FAHRZEUG WHERE FZ_ID = ?";

		boolean oldAuto = true;
		try {
			Connection con = getConnection();          // wirft DataException, falls Connection fehlt
			oldAuto = con.getAutoCommit();
			con.setAutoCommit(false);                   // Transaktion starten

			try (PreparedStatement ps = con.prepareStatement(deleteMaut)) {
				ps.setLong(1, fz_id);
				ps.executeUpdate();
			}
			try (PreparedStatement ps = con.prepareStatement(deleteGeraet)) {
				ps.setLong(1, fz_id);
				ps.executeUpdate();
			}

			int affected;
			try (PreparedStatement ps = con.prepareStatement(deleteFzg)) {
				ps.setLong(1, fz_id);
				affected = ps.executeUpdate();
			}
			if (affected == 0) {
				// Fahrzeug gab es nicht – Test 5 prüft das nicht direkt,
				// aber sauberes Fehlersignal ist sinnvoll:
				throw new DataException("Fahrzeug nicht gefunden: FZ_ID=" + fz_id);
			}

			con.commit();
			L.info("Fahrzeug gelöscht: FZ_ID={}", fz_id);
		} catch (Exception e) {
			try {
				getConnection().rollback();
			} catch (Exception ignore) {
			}
			L.error("deleteVehicle failed fz_id={}", fz_id, e);
			throw new DataException("Fehler beim Löschen des Fahrzeugs", e);
		} finally {
			try {
				getConnection().setAutoCommit(oldAuto);
			} catch (Exception ignore) {
			}
		}
	}

	@Override
	public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
		// TODO Auto-generated method stub
		return null;
	}

}
