using CsvHelper;
using CsvHelper.Configuration;
using System.Diagnostics;
using System.Globalization;

namespace CheckPhoneNumber
{
    public class Program
    {
        private static string? _baseFilePath;
        private static string? _checkFilePath;
        private static string? _resultFilePath;
        private static string? _cleanedResultFilePath;

        static async Task Main(string[] args)
        {
            _baseFilePath = args.Length > 0 ? args[0] : "input.csv"; // Путь к файлу с базой номеров телефонов
            _checkFilePath = args.Length > 1 ? args[1] : "check.txt"; // Путь к файлу с номерами телефонов для проверки
            _resultFilePath = "result.txt"; // Путь к файлу для сохранения результатов
            _cleanedResultFilePath = "cleaned_result.txt"; // Путь к файлу для сохранения очищенных результатов

            // Засекаем время выполнения
            Stopwatch stopwatch = Stopwatch.StartNew();

            // Загрузка базы номеров телефонов
            HashSet<string> phoneNumbers = await LoadBaseFileAsync(_baseFilePath);

            // Проверка номеров из второго файла и сохранение результатов
            await CheckAndSaveNumbersAsync(phoneNumbers, _checkFilePath, _resultFilePath);

            // Очистка дубликатов в файле с результатами
            await RemoveDuplicatesAsync(_resultFilePath, _cleanedResultFilePath);

            // Останавливаем счетчик времени
            stopwatch.Stop();

            // Выводим время выполнения
            //Console.WriteLine($"Execution time: {stopwatch.ElapsedMilliseconds} milliseconds");

            // Получаем время выполнения в виде TimeSpan
            TimeSpan executionTime = TimeSpan.FromMilliseconds(stopwatch.ElapsedMilliseconds);
            // Выводим время выполнения в формате "часы:минуты:секунды"
            Console.WriteLine($"Execution time: {executionTime.Hours} hours, {executionTime.Minutes} minutes, {executionTime.Seconds} seconds");

            Console.WriteLine("Done!");
        }

        static async Task<HashSet<string>> LoadBaseFileAsync(string filePath)
        {
            HashSet<string> phoneNumbers = new HashSet<string>();
            long counter = 0;

            using (var streamReader = new StreamReader(filePath))
            using (var csvReader = new CsvReader(streamReader, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                // Настройка буферизации чтения
                //BufferSize = 4096,
                HasHeaderRecord = false, // Если нет заголовка в файле
                BadDataFound = arg => phoneNumbers.Add(arg.Context.Parser.RawRecord)
            }))
            {
                while (await csvReader.ReadAsync())
                {
                    string phoneNumber = csvReader.GetField<string>(0);
                    string[] parts = phoneNumber.Split('\t');
                    if (parts.Length > 1)
                    {
                        phoneNumber = parts[1].Replace("\"", "").Trim();
                    }
                    phoneNumbers.Add(phoneNumber);

                    counter++;

                    if (counter % 1000000 == 0)
                    {
                        Console.WriteLine($"Read {counter / 1000000} million phone numbers...");
                    }

                    if (counter % 10000000 == 0)
                    {
                        await CheckAndSaveNumbersAsync(phoneNumbers, _checkFilePath, _resultFilePath);
                        phoneNumbers = new HashSet<string>(); // Создаем новый объект HashSet для очистки памяти
                    }
                }
            }

            // Проверяем оставшиеся номера после завершения цикла
            if (phoneNumbers.Count > 0)
            {
                await CheckAndSaveNumbersAsync(phoneNumbers, _checkFilePath, _resultFilePath);
            }

            return phoneNumbers;
        }

        static async Task CheckAndSaveNumbersAsync(HashSet<string> phoneNumbers, string checkFilePath, string resultFilePath)
        {
            using (StreamWriter writer = new StreamWriter(resultFilePath, true))
            {
                using (StreamReader checkReader = new StreamReader(checkFilePath))
                {
                    string phoneNumber;
                    while ((phoneNumber = await checkReader.ReadLineAsync()) != null)
                    {
                        if (phoneNumbers.Contains(phoneNumber.Trim()))
                        {
                            await writer.WriteLineAsync(phoneNumber.Trim()); // Сохраняем номер в результат
                        }
                    }
                }
            }
        }

        static async Task RemoveDuplicatesAsync(string inputFilePath, string outputFilePath)
        {
            HashSet<string> uniqueLines = new HashSet<string>();

            using (StreamReader reader = new StreamReader(inputFilePath))
            using (StreamWriter writer = new StreamWriter(outputFilePath))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (uniqueLines.Add(line))
                    {
                        await writer.WriteLineAsync(line);
                    }
                }
            }
        }
    }
}
