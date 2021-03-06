package com.vzard.kafclient.handlers;


import com.vzard.kafclient.excephandler.DefaultExceptionHandler;
import com.vzard.kafclient.excephandler.ExceptionHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public abstract class SafelyMessageHandler implements MessageHandler {

    private List<ExceptionHandler> exceptionHandlers = new ArrayList<ExceptionHandler>();

    {
        exceptionHandlers.add(new DefaultExceptionHandler());

    }


    public SafelyMessageHandler() {

    }

    public SafelyMessageHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandlers.add(exceptionHandler);

    }


    public SafelyMessageHandler(List<ExceptionHandler> exceptionHandlers) {
        this.exceptionHandlers.addAll(exceptionHandlers);
    }

    protected void handleException(Throwable t, String message) {
        for (ExceptionHandler exceptionHandler : exceptionHandlers) {
            if (t.getClass() == IllegalStateException.class
                    && t.getClass() != null
                    && t.getCause().getClass() == InvocationTargetException.class
                    && t.getCause().getCause() != null) {

                t = t.getCause().getCause();

            }


        }

    }


    protected abstract void doExecute(String message);





}
