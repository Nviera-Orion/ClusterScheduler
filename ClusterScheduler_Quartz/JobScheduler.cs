using log4net;
using Quartz;
using Quartz.Impl;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClusterScheduler_Quartz
{
    public   class JobScheduler
    {
        // construct a scheduler factory   
        static NameValueCollection props = new NameValueCollection { { "quartz.serializer.type", "binary" } };
        static  StdSchedulerFactory schedFact = new StdSchedulerFactory(props);
        public static async Task  Start(ILog log)
        {

            // get a scheduler
            IScheduler sched = await schedFact.GetScheduler();
             await sched.Start();

            // define the job and tie it to our Job class
            IJobDetail job = JobBuilder.Create<PollingJob>()
                .WithIdentity("BlobJob", "PollingJobgroup")
                .Build();

            // Trigger the job to run now, and then every 40 seconds
            ITrigger trigger = TriggerBuilder.Create()
              .WithIdentity("BlobTrigger", "PollingJobgroup")
              .StartNow()
              .WithSimpleSchedule(x => x
                  .WithIntervalInMinutes(2)
                  .RepeatForever())
              .Build();

          log.Debug("Starting the Polling Job trigger for every 10 sec");
          await  sched.ScheduleJob(job, trigger);
        }

        public static async void Stop()
        {
            IScheduler sched = await schedFact.GetScheduler();
            await sched.Shutdown();
        }

        public static async void Pause()
        {
            IScheduler sched = await schedFact.GetScheduler();
            await sched.PauseAll();
        }


        public static async void Resume()
        {
            IScheduler sched = await schedFact.GetScheduler();
            await sched.ResumeAll();
        }


    }
}
