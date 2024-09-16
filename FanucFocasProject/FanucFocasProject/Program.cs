using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace FanucFocasProject
{

    class Program
    {
        static ushort _handle = 0;              // class variable
        static short _ret = 0;                  // holds return values
        static void Main(string[] args)
        {
            //
            _ret = Focas1.cnc_allclibhndl3("192.168.0.100", 8193, 6, out _handle);          // 6 is the timeout, might need to adjust accordingly
        
            if (_ret != Focas1.EW_OK)
            {
                Console.WriteLine($"Unable to connevt to 192.168.0.100 on port 8193 \n\nReturn Code: {_ret}\n\nExiting...");
                Console.Read();                 // prevents command prompt from closing on us
            }
            else
            {
                Console.WriteLine($"Our Focas handle is {_handle}");

                string mode = GetMode();
                Console.WriteLine($"\n\nMode is: {mode}");

                string stat = GetStatus();
                Console.WriteLine($"\n\Status is: {stat}");
                
                Console.Read();                 // prevents command prompt from closing on us
            };
        }

        public static string GetMode()
        {
            if (_handle == 0)
            {
                Console.WriteLine("Error: Please obtain a handle before calling this method");
                return "";
            }

            // variable Mode holds an instance of the Focas1.ODBST class
            Focas1.ODBST Mode = new Focas1.ODBST();         // class in the FOCAS1 library used to store information about the CNC machine's mode or status
            
            _ret = Focas1.cnc_statinfo(_handle, Mode);
            
            if (_ret!=0)
            {
                Console.WriteLine($"Error: Unable to obtain mode. \nReturn Code: {_ret}");
                return "";
            }
            
            string modestr = ModeNumberToString(Mode.aut);
            return $"Mode is: {modestr}";
        }

        public static string ModeNumberToString(int num){
            switch (num)
            {
                case 0: { return "MDI"; }
                case 1: { return "MEM"; }
                case 3: { return "EDIT"; }
                case 4: { return "HND"; }
                case 5: { return "JOG"; }
                case 6: { return "Teach in JOG"; }
                case 7: { return "Teach in HND"; }
                case 8: { return "INC"; }
                case 9: { return "REF"; }
                case 10: { return "RMT"; }
                default: { return "UNAVAILABLE"; }
            }
        }

        public static string GetStatus()
        {
            if (_handle == 0)
            {
                Console.WriteLine("Error: Please obtain a handle before calling this method");
                return "";
            }

            // variable Mode holds an instance of the Focas1.ODBST class
            Focas1.ODBST Status = new Focas1.ODBST();         // class in the FOCAS1 library used to store information about the CNC machine's mode or status
            
            _ret = Focas1.cnc_statinfo(_handle, Status);
            
            if (_ret!=0)
            {
                Console.WriteLine($"Error: Unable to obtain status. \nReturn Code: {_ret}");
                return "";
            }

            return $"Status is: {Status.run}";
        }

        public static string StatusNumberToString(int num)
        {
            switch (num)
            {
                case 0: { return "****"; }
                case 1: { return "STOP"; }
                case 2: { return "HOLD"; }
                case 3: { return "STRT"; }
                case 4: { return "MSTR"; }
                default: { return "UNAVAILABLE"; }
            }
        }
    }
}