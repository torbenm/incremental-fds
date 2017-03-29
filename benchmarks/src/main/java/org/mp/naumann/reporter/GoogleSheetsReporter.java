package org.mp.naumann.reporter;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;

public class GoogleSheetsReporter implements Reporter {

    private final String APPLICATION_NAME =
            "Incremental Function Dependency Algorithms";

    /**
     * Directory to store user credentials for this application.
     */
    private final File DATA_STORE_DIR = new File(
            System.getProperty("user.home"), ".credentials/incremental-fd-benchmarks");
    /**
     * Global instance of the JSON factory.
     */
    private final JsonFactory JSON_FACTORY =
            JacksonFactory.getDefaultInstance();
    private final Sheets sheetService;
    private final String spreadsheetId;
    private final String spreadsheet;
    /**
     * Global instance of the scopes required by this quickstart.
     *
     * If modifying these scopes, delete your previously saved credentials
     * at ~/.credentials/sheets.googleapis.com-java-quickstart
     */
    private final List<String> SCOPES =
            Collections.singletonList(SheetsScopes.SPREADSHEETS);
    /**
     * Global instance of the {@link FileDataStoreFactory}.
     */
    private FileDataStoreFactory DATA_STORE_FACTORY;
    /**
     * Global instance of the HTTP transport.
     */
    private HttpTransport HTTP_TRANSPORT;

    {
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    public GoogleSheetsReporter(String spreadsheetId, String spreadsheet) throws IOException {
        this.spreadsheet = spreadsheet;
        sheetService = getSheetsService();
        this.spreadsheetId = spreadsheetId;
    }

    /**
     * Creates an authorized Credential object.
     *
     * @return an authorized Credential object.
     */
    private Credential authorize() throws IOException {
        // Load client secrets.
        InputStream in =
                GoogleSheetsReporter.class.getResourceAsStream("/client_secret.json");
        GoogleClientSecrets clientSecrets =
                GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow =
                new GoogleAuthorizationCodeFlow.Builder(
                        HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                        .setDataStoreFactory(DATA_STORE_FACTORY)
                        .setAccessType("offline")
                        .build();
        Credential credential = new AuthorizationCodeInstalledApp(
                flow, new LocalServerReceiver()).authorize("user");
        System.out.println(
                "Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
        return credential;
    }

    /**
     * Build and return an authorized Sheets API client service.
     *
     * @return an authorized Sheets API client service
     */
    private Sheets getSheetsService() throws IOException {
        Credential credential = authorize();
        return new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    public void writeNewLine(Object... values) throws IOException {
        String range = spreadsheet + "!A:I";
        ValueRange vr = new ValueRange().setValues(Collections.singletonList(Lists.newArrayList(values)));
        sheetService.spreadsheets()
                .values()
                .append(spreadsheetId, range, vr)
                .setValueInputOption("RAW")
                .execute();
    }


}
