package org.dcache;

import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Enumeration;

public class FileServlet extends HttpServlet {
    private static final Logger _log = LoggerFactory.getLogger(FileServlet.class);

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        FileInputStream fileIn = null;
        OutputStream outputStream = null;
        String filepath = "";

        try {
            filepath = request.getHeader("file");
        } catch (NullPointerException e) {
            response.setStatus(HttpStatus.BAD_REQUEST_400);
            return;
        }

        if(filepath.equals("")) {
            response.setStatus(HttpStatus.BAD_REQUEST_400);
            return;
        }
        File file = new File(filepath);

        if (!file.exists() || file.isDirectory()) {
            response.setStatus(HttpStatus.NOT_FOUND_404);
            return;
        }

        try {
            fileIn = new FileInputStream(file);
            outputStream = response.getOutputStream();

            response.setContentType("application/octet-stream");
            fileIn.transferTo(outputStream);
            response.setStatus(HttpStatus.OK_200);
        } catch (IOException e) {
            _log.error("Error while transferring file to client: ", e);
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        } finally {
            if(fileIn != null) {
                fileIn.close();
            }

            if(outputStream != null) {
                outputStream.close();
            }
        }
    }
}
