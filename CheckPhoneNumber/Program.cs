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

        private class ValueCsv
        {
            public string? Value { get; set; }
            public string? Phone { get; set; }

            public override string ToString()
            {
                return $"{Value}; {Phone};";
            }
        }

        static async Task Main(string[] args)
        {
            _baseFilePath = args.Length > 0 ? args[0] : "input.csv"; // Путь к файлу с базой номеров телефонов
            _checkFilePath = args.Length > 1 ? args[1] : "check.txt"; // Путь к файлу с номерами телефонов для проверки
            _resultFilePath = "result.txt"; // Путь к файлу для сохранения результатов
            _cleanedResultFilePath = "cleaned_result.txt"; // Путь к файлу для сохранения очищенных результатов

            // Засекаем время выполнения
            Stopwatch stopwatch = Stopwatch.StartNew();

            // Загрузка базы номеров телефонов
            HashSet<ValueCsv> phoneNumbers = await LoadBaseFileAsync(_baseFilePath);

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

        static async Task<HashSet<ValueCsv>> LoadBaseFileAsync(string filePath)
        {
            HashSet<ValueCsv> phoneNumbers = new HashSet<ValueCsv>();
            long counter = 0;

            using (var streamReader = new StreamReader(filePath))
            using (var csvReader = new CsvReader(streamReader, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                // Настройка буферизации чтения
                //BufferSize = 4096,
                HasHeaderRecord = false, // Если нет заголовка в файле
                BadDataFound = arg => { }
            }))
            {
                while (await csvReader.ReadAsync())
                {
                    string phoneNumber = csvReader.GetField<string>(0);
                    string[] parts = phoneNumber.Split('\t');
                    string? value = null; 
                    if (parts.Length > 1)
                    {
                        value = parts[0].Replace("\"", "").Trim();
                        phoneNumber = parts[1].Replace("\"", "").Trim();
                    }
                    phoneNumbers.Add(new ValueCsv() { Value = value, Phone = phoneNumber });

                    counter++;

                    if (counter % 1000000 == 0)
                    {
                        Console.WriteLine($"Read {counter / 1000000} million phone numbers...");
                    }

                    if (counter % 10000000 == 0)
                    {
                        await CheckAndSaveNumbersAsync(phoneNumbers, _checkFilePath, _resultFilePath);
                        phoneNumbers = new HashSet<ValueCsv>(); // Создаем новый объект HashSet для очистки памяти
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

        static async Task CheckAndSaveNumbersAsync(HashSet<ValueCsv> phoneNumbers, string checkFilePath, string resultFilePath)
        {
            // Создаем словарь для хранения объектов ValueCsv по номерам телефонов
            var phoneDictionary = new Dictionary<string, List<string>>();

            // Заполняем словарь из phoneNumbers
            foreach (var item in phoneNumbers)
            {
                if (!phoneDictionary.ContainsKey(item.Phone))
                {
                    phoneDictionary.Add(item.Phone, new List<string>());
                }
                phoneDictionary[item.Phone].Add(item.Value);
            }

            using (StreamWriter writer = new StreamWriter(resultFilePath, true))
            using (StreamReader checkReader = new StreamReader(checkFilePath))
            {
                string phoneNumber;
                while ((phoneNumber = await checkReader.ReadLineAsync()) != null)
                {
                    phoneNumber = phoneNumber.Trim(); // Убираем лишние пробелы вокруг номера
                    if (phoneDictionary.TryGetValue(phoneNumber, out var values))
                    {
                        // Если номер найден в словаре, записываем все соответствующие значения в файл результатов
                        foreach (var value in values)
                        {
                            await writer.WriteLineAsync($"{value}; {phoneNumber};");
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
