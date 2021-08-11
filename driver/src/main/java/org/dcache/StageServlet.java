package org.dcache;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.*;
import java.nio.file.*;

@MultipartConfig
public class StageServlet extends HttpServlet {
    private static final Logger _log = LoggerFactory.getLogger(FileServlet.class);

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String filepath;
        _log.debug("Getting filepath");
        try {
            try {
                filepath = request.getHeader("file");
            } catch (NullPointerException e) {
                response.setStatus(HttpStatus.BAD_REQUEST_400);
                _log.error("No 'file' given in header.");
                return;
            }

            if (filepath.equals("")) {
                response.setStatus(HttpStatus.BAD_REQUEST_400);
                _log.error("'file' in header is empty.");
                return;
            }

            File file = new File(filepath);
            if (file.isDirectory() || file.exists()) {
                response.setStatus(HttpStatus.BAD_REQUEST_400);
                _log.error("The given filepath is a directory or already exists.");
                return;
            }

            _log.debug("Filepath is {}", filepath);
            response.setContentType("text/plain;charset=UTF-8");

            ServletOutputStream out = response.getOutputStream();

            if (!file.createNewFile()) {
                _log.error("File {} already exists.", filepath);
                response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                return;
            } else {
                _log.info("File created {}", filepath);
            }

            for (Part part: request.getParts()) {
                try (InputStream inputStream = part.getInputStream();
                     OutputStream outputStream = Files.newOutputStream(Path.of(filepath), StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING)) {
                    IO.copy(inputStream, outputStream);
                    out.print("Saved part["+ part.getName() + "] to " + filepath);
                    _log.debug("Saved part["+ part.getName() + "] to " + filepath);
                }
            }

            out.print("File successfully uploaded");
            _log.info("File {} was successfully uploaded", filepath);
        } catch (ServletException e) {
            _log.warn("Could not get fileparts: ", e);
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
            ServletOutputStream out = response.getOutputStream();
            out.print(e.toString());
        }
    }

}
