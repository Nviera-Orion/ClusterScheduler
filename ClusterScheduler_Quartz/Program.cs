using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClusterScheduler_Quartz
{
    class Program
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();          
            startPipe();
            Console.WriteLine("Enter X to stop the Polling ");
            if(Console.ReadLine().ToString().ToUpper() =="X")
                JobScheduler.Stop();

        }

        public static async void startPipe()
        {
            //log.Debug("Starting the Polling job");
            await JobScheduler.Start(log);
        }
       
    }
}



//Q's
//
// 1. How do we make sure pig job ran succesfully
