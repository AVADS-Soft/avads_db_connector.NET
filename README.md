# avads_db_connector.NET
Реализация клиента AVADS TCP на .NET

Клиентское API для взаимодействия с Сервером Архивирования AVADS. Содержит методы для инициализации TCP подключения, взаимодействию с базами, рядами и данными.

## Начало работы:

1) Для подключения необходимо создать экземпляр класса ```TsdbCredentials(string ip, int port, string login, string password)``` с параметрами инициализации. Например, c параметрами по умолчанию:

  ```
    public static TsdbCredentials Credentials = new TsdbCredentials("127.0.0.1", 7777, "admin", "admin");
  ```

2) Далее создается экземпляр клиента, содержащий API методы.

  ```
    using var client = new TsdbClient(Credentials);
  ```

3) После создания, необходимо инициализировать TCP подключение:

  ```
    await client.Init();
  ```

4) Используйте клиента для необходимой логики. 

Например:


4.1. Получение списка баз

```
    var list = await client.GetBasesList();
```

4.2. Создание базы

```
    var newBase = new BaseT(baseName, "db/test", "10mb");
    await client.CreateBase(newBase);
```

4.3. Удаление ряда

  ```
    await client.RemoveSeries(baseEx.Name, seriesId);
```

4.4. Запрос объекта границ данных ряда:

  ```
    var bounds = await client.DataGetBoundary(baseName, seriesEx.Id);
```

Более подробно c API c примерами можно ознакомиться в проекте TSDBConnectorTest
