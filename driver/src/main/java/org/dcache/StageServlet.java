package org.dcache;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.*;
import java.nio.file.*;

/*
 * Servlet to accept POST-Requests for uploading files directly into dCache
 * to complete StageRequests
 */
@MultipartConfig
public class StageServlet extends HttpServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileServlet.class);

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String filepath;
        AsyncContext asyncContext = request.startAsync();

        LOGGER.debug("Getting filepath");
        try {
            filepath = request.getHeader("file");

            if(filepath == null || filepath.equals("")) {
                response.setStatus(HttpStatus.BAD_REQUEST_400);
                asyncContext.complete();
                return;
            }

            File file = new File(filepath);
            if (file.isDirectory() || file.exists()) {
                response.setStatus(HttpStatus.BAD_REQUEST_400);
                LOGGER.error("The given filepath is a directory or already exists.");
                asyncContext.complete();
                return;
            }

            LOGGER.debug("Filepath is {}", filepath);
            response.setContentType("text/plain;charset=UTF-8");

            ServletOutputStream out = response.getOutputStream();

            if (!file.createNewFile()) {
                LOGGER.error("File {} already exists.", filepath);
                response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                asyncContext.complete();
                return;
            }
            LOGGER.info("File created {}", filepath);
            String progressMessage;

            for (Part part: request.getParts()) {
                try (InputStream inputStream = part.getInputStream();
                     OutputStream outputStream = Files.newOutputStream(Path.of(filepath), StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING)) {
                    IO.copy(inputStream, outputStream);
                    progressMessage = "Saved part["+ part.getName() + "] to " + filepath;
                    out.print(progressMessage);
                    LOGGER.debug(progressMessage);
                }
            }

            out.print("File successfully uploaded");
            LOGGER.info("File {} was successfully uploaded", filepath);
            response.setStatus(HttpStatus.CREATED_201);
            asyncContext.complete();
        } catch (ServletException e) {
            LOGGER.error("Could not get fileparts: {}", e.toString());
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
            ServletOutputStream out = response.getOutputStream();
            out.print(e.toString());
            asyncContext.complete();
        }
    }

}
