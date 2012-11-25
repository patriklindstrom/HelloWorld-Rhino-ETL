using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FileHelpers;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Files;
using Rhino.Etl.Core.Operations;

namespace HelloRhinoEtl
{

    /// <summary>
    /// The Data Class that represent each row. Notice the DelimetedRecord annotation. That is from the File Helper
    /// </summary>
    [DelimitedRecord(",")] 
    public class DataRecord
    {
        public int Id;
        public string AWord;
    }
    /// <summary>
    /// Just get data from a File. Could be database or fake constructed data
    /// </summary>
    public class SimpleFileDataGet  : AbstractOperation
    {
        public SimpleFileDataGet(string inPutFilepath)
        {
            FilePath = inPutFilepath;
        }
        public string FilePath { get; set; }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        { 

            using (FileEngine file = FluentFile.For<DataRecord>().From(FilePath))
            {
                foreach (object obj in file)
                {
                    yield return Row.FromObject(obj);
                }
            }
        }
    }

    public class TransformWord :AbstractOperation
{
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                var revWord = (string)row["AWord"];
                row["AWord"] = new string(revWord.ToCharArray().Reverse().ToArray());                                 
                yield return row;
            }            
        }
}

    public class JoinWordLists : JoinOperation
    {
        protected override void SetupJoinConditions()
        {
            InnerJoin
                .Left("Id")
                .Right("Id");
        }

        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            Row row = leftRow.Clone();
            row["AWord"] = leftRow["AWord"].ToString() + " " +
                                       rightRow["AWord"].ToString();
            return row;
        }
    }

    /// <summary>
    /// We will just put data on the screen. Would be more realistic to put to other file or database
    /// </summary>
    public class PutData : AbstractOperation
    {
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                var record = new DataRecord
                    {
                        Id = (int) row["Id"],
                        AWord = (string)row["AWord"]
                    };
                Console.WriteLine(record.AWord);
            }
            yield break;
        }
    }
    /// <summary>
    /// Here is the actual ETL process where all steps are registred. 
    /// It represent one dataflow, We get two datasources Join them, Transform data and put it somewhere.
    /// </summary>
    public class ExNihiloProcess : EtlProcess
    {
        protected override void Initialize()
        {    // my path to the file is D:\Users\Patrik\Documents\GitHub\HelloWorld-Rhino-ETL\HelloRhinoEtl\UntransformedWordList.csv
            //Relative Path is for me : ..\..\..\UntransformedWordList1.csv
            //A hash join operation between the files on the id
            Register(new JoinWordLists()
                .Left(new SimpleFileDataGet(@"..\..\..\UntransformedWordList1.csv"))
                .Right(new SimpleFileDataGet(@"..\..\..\UntransformedWordList2.csv")));
            // A silly Transformation of each row
            Register(new TransformWord());
            //Put the data on the screen. Should normally be file or database table
            Register(new PutData());
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("----Lets create a Rhino-ETL ----");
            Console.WriteLine("--------------------------------");
            // Here is the actual work. 
            var exNihiloP = new ExNihiloProcess();
            exNihiloP.Execute();
            Console.WriteLine("-------------------------------");
            Console.WriteLine("----Hit any Rhino to exit------");
            Console.ReadKey();

        }
    }
}
