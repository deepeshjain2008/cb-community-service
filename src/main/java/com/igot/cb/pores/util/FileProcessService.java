package com.igot.cb.pores.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.igot.cb.pores.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FileProcessService {

  public List<Map<String, String>> processCsvAndSendMessage(
      InputStream inputStream) throws IOException {
    log.info("FileProcessService::processCsvAndSendMessage");
    List<Map<String, String>> dataRows = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

      List<String> headers = csvParser.getHeaderNames();

      for (CSVRecord csvRecord : csvParser) {
        boolean allBlank = true;
        Map<String, String> rowData = new HashMap<>();
        for (String header : headers) {
          String cellValue = csvRecord.get(header);
          if (cellValue != null && !cellValue.trim().isEmpty()) {
            // Replace newlines and trim the value
            cellValue = cellValue.replace("\n", ",").trim();
            allBlank = false;
          }
          rowData.put(header, cellValue);
        }
        if (allBlank) {
          break; // If all cells are blank in the current row, stop processing
        }
        dataRows.add(rowData);
      }
      log.info("Number of Data Rows Processed: {}", dataRows.size());
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return dataRows;
  }

}
