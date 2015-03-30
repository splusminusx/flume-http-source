package ru.livetex.flume;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.net.ssl.SSLServerSocket;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.tools.HTTPServerConstraintUtil;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpSource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSource.class);
    private volatile Integer port;
    private volatile Server srv;
    private volatile String host;
    private HTTPSourceHandler handler;
    private SourceCounter sourceCounter;
    private volatile String keyStorePath;
    private volatile String keyStorePassword;
    private volatile Boolean sslEnabled;
    private final List<String> excludedProtocols = new LinkedList();

    public HttpSource() {
    }

    public void configure(Context context) {
        try {
            this.sslEnabled = context.getBoolean("enableSSL", Boolean.valueOf(false));
            this.port = context.getInteger("port");
            this.host = context.getString("bind", "0.0.0.0");
            Preconditions.checkState(this.host != null && !this.host.isEmpty(), "HttpSource hostname specified is empty");
            Preconditions.checkNotNull(this.port, "HttpSource requires a port number to be specified");
            String ex = context.getString("handler", "org.apache.flume.source.http.JSONHandler").trim();
            if(this.sslEnabled.booleanValue()) {
                LOG.debug("SSL configuration enabled");
                this.keyStorePath = context.getString("keystore");
                Preconditions.checkArgument(this.keyStorePath != null && !this.keyStorePath.isEmpty(), "Keystore is required for SSL Conifguration");
                this.keyStorePassword = context.getString("keystorePassword");
                Preconditions.checkArgument(this.keyStorePassword != null, "Keystore password is required for SSL Configuration");
                String clazz = context.getString("excludeProtocols");
                if(clazz == null) {
                    this.excludedProtocols.add("SSLv3");
                } else {
                    this.excludedProtocols.addAll(Arrays.asList(clazz.split(" ")));
                    if(!this.excludedProtocols.contains("SSLv3")) {
                        this.excludedProtocols.add("SSLv3");
                    }
                }
            }

            Class clazz1 = Class.forName(ex);
            this.handler = (HTTPSourceHandler)clazz1.getDeclaredConstructor(new Class[0]).newInstance(new Object[0]);
            ImmutableMap subProps = context.getSubProperties("handler.");
            this.handler.configure(new Context(subProps));
        } catch (ClassNotFoundException var5) {
            LOG.error("Error while configuring HttpSource. Exception follows.", var5);
            Throwables.propagate(var5);
        } catch (ClassCastException var6) {
            LOG.error("Deserializer is not an instance of HttpSourceHandler.Deserializer must implement HttpSourceHandler.");
            Throwables.propagate(var6);
        } catch (Exception var7) {
            LOG.error("Error configuring HttpSource!", var7);
            Throwables.propagate(var7);
        }

        if(this.sourceCounter == null) {
            this.sourceCounter = new SourceCounter(this.getName());
        }

    }

    private void checkHostAndPort() {
        Preconditions.checkState(this.host != null && !this.host.isEmpty(), "HttpSource hostname specified is empty");
        Preconditions.checkNotNull(this.port, "HttpSource requires a port number to be specified");
    }

    public void start() {
        Preconditions.checkState(this.srv == null, "Running HTTP Server found in source: " + this.getName() + " before I started one." + "Will not attempt to start.");
        this.srv = new Server();
        Connector[] connectors = new Connector[1];
        if(this.sslEnabled.booleanValue()) {
            HttpSource.HttpSourceSocketConnector ex = new HttpSource.HttpSourceSocketConnector(this.excludedProtocols);
            ex.setKeystore(this.keyStorePath);
            ex.setKeyPassword(this.keyStorePassword);
            ex.setReuseAddress(true);
            connectors[0] = ex;
        } else {
            SelectChannelConnector ex1 = new SelectChannelConnector();
            ex1.setReuseAddress(true);
            connectors[0] = ex1;
        }

        connectors[0].setHost(this.host);
        connectors[0].setPort(this.port.intValue());
        this.srv.setConnectors(connectors);

        try {
            org.mortbay.jetty.servlet.Context ex2 = new org.mortbay.jetty.servlet.Context(this.srv, "/", 1);
            ex2.addServlet(new ServletHolder(new HttpSource.FlumeHTTPServlet()), "/");
            HTTPServerConstraintUtil.enforceConstraints(ex2);
            this.srv.start();
            Preconditions.checkArgument(this.srv.getHandler().equals(ex2));
        } catch (Exception var3) {
            LOG.error("Error while starting HttpSource. Exception follows.", var3);
            Throwables.propagate(var3);
        }

        Preconditions.checkArgument(this.srv.isRunning());
        this.sourceCounter.start();
        super.start();
    }

    public void stop() {
        try {
            this.srv.stop();
            this.srv.join();
            this.srv = null;
        } catch (Exception var2) {
            LOG.error("Error while stopping HttpSource. Exception follows.", var2);
        }

        this.sourceCounter.stop();
        LOG.info("Http source {} stopped. Metrics: {}", this.getName(), this.sourceCounter);
    }

    private static class HttpSourceSocketConnector extends SslSocketConnector {
        private final List<String> excludedProtocols;

        HttpSourceSocketConnector(List<String> excludedProtocols) {
            this.excludedProtocols = excludedProtocols;
        }

        public ServerSocket newServerSocket(String host, int port, int backlog) throws IOException {
            SSLServerSocket socket = (SSLServerSocket)super.newServerSocket(host, port, backlog);
            String[] protocols = socket.getEnabledProtocols();
            ArrayList newProtocols = new ArrayList(protocols.length);
            String[] arr$ = protocols;
            int len$ = protocols.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                String protocol = arr$[i$];
                if(!this.excludedProtocols.contains(protocol)) {
                    newProtocols.add(protocol);
                }
            }

            socket.setEnabledProtocols((String[])newProtocols.toArray(new String[newProtocols.size()]));
            return socket;
        }
    }

    private class FlumeHTTPServlet extends HttpServlet {
        private static final long serialVersionUID = 4923924863218790344L;

        private FlumeHTTPServlet() {
        }

        public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
            List events = Collections.emptyList();

            try {
                events = HttpSource.this.handler.getEvents(request);
            } catch (HTTPBadRequestException var7) {
                HttpSource.LOG.warn("Received bad request from client. ", var7);
                response.sendError(400, "Bad request from client. " + var7.getMessage());
                return;
            } catch (Exception var8) {
                HttpSource.LOG.warn("Deserializer threw unexpected exception. ", var8);
                response.sendError(500, "Deserializer threw unexpected exception. " + var8.getMessage());
                return;
            }

            HttpSource.this.sourceCounter.incrementAppendBatchReceivedCount();
            HttpSource.this.sourceCounter.addToEventReceivedCount((long)events.size());

            try {
                HttpSource.this.getChannelProcessor().processEventBatch(events);
            } catch (ChannelException var5) {
                HttpSource.LOG.warn("Error appending event to channel. Channel might be full. Consider increasing the channel capacity or make sure the sinks perform faster.", var5);
                response.sendError(503, "Error appending event to channel. Channel might be full." + var5.getMessage());
                return;
            } catch (Exception var6) {
                HttpSource.LOG.warn("Unexpected error appending event to channel. ", var6);
                response.sendError(500, "Unexpected error while appending event to channel. " + var6.getMessage());
                return;
            }

            response.setCharacterEncoding(request.getCharacterEncoding());
            response.setStatus(200);
            response.setHeader("Access-Control-Allow-Origin", request.getHeader("origin"));
            response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
            response.setHeader("Access-Control-Allow-Credentials", "true");
            response.flushBuffer();
            HttpSource.this.sourceCounter.incrementAppendBatchAcceptedCount();
            HttpSource.this.sourceCounter.addToEventAcceptedCount((long)events.size());
        }

        public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
            this.doPost(request, response);
        }
    }
}
