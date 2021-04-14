package com.aikfk.flink.cep.project;

import com.aikfk.flink.cep.project.event.MonitorEvent;
import com.aikfk.flink.cep.project.event.PowerEvent;
import com.aikfk.flink.cep.project.event.TemperatureEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class MonitorEventSource extends RichParallelSourceFunction<MonitorEvent> {

    private  boolean running = true ;
    private  int maxRackID;
    private  long pause ;
    private  double temperatureRation ;
    private  double temperatureStd ;
    private  double temperatureMean ;
    private  double powerStd ;
    private  double powermean ;
    private  int shard ;
    private  int offset ;
    private Random random ;


    public MonitorEventSource(int maxRackID,
                              long pause,
                              double temperatureRation,
                              double temperatureStd,
                              double temperatureMean,
                              double powerStd,
                              double powermean
                              ) {
        this.maxRackID = maxRackID;
        this.pause = pause;
        this.temperatureRation = temperatureRation;
        this.temperatureStd = temperatureStd;
        this.temperatureMean = temperatureMean;
        this.powerStd = powerStd;
        this.powermean = powermean;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        random = new Random();

        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        offset = (int)((double)maxRackID / numberOfParallelSubtasks * indexOfThisSubtask);
        shard = (int)((double)maxRackID / numberOfParallelSubtasks * (indexOfThisSubtask +1)) - offset;

    }

    @Override
    public void run(SourceContext<MonitorEvent> sourceContext) throws Exception {


        while (running){
            MonitorEvent monitorEvent ;
            int rackID = random.nextInt(shard) + offset ;

            if (random.nextDouble() < temperatureRation){
                double temperature = random.nextGaussian() * temperatureStd + temperatureMean ;
                monitorEvent = new TemperatureEvent(rackID , temperature);
            }else {
                double power = random.nextGaussian() * powerStd + powermean ;
                monitorEvent = new PowerEvent(rackID  ,power);
            }

            monitorEvent.setCurrentTime(System.currentTimeMillis());

            sourceContext.collect(monitorEvent);

            Thread.sleep(pause);

        }

    }

    @Override
    public void cancel() {
        running =false;
    }
}
