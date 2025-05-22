-- Направете си нова база данни, наречена {HANDLER}_ZOO_DB.

CREATE DATABASE TIGER_ZOO_DB

-- Създайте схеми (ако е необходимо), за различните обекти

-- Няма нужда да създавате Stage - можете да заредите данните, които ще намерите във файла директно.

-- 1. Профил на зоопарка
-- Да се извлекат официалното име на зоологическата градина (zooName) и нейното местоположение (location)
.

CREATE OR REPLACE SCHEMA zoo_schema;
USE SCHEMA zoo_schema;


CREATE TABLE TIGER_ZOO_DB.PUBLIC.raw_data(
    json_raw_data VARIANT
)

SELECT*
FROM TIGER_ZOO_DB.PUBLIC.raw_data

SELECT json_raw_data:zooName, json_raw_data:location
FROM TIGER_ZOO_DB.PUBLIC.raw_data


-- CREATE TABLE  TIGER_ZOO_DB.PUBLIC.zoo(
--     zooId INT PRIMARY KEY,
--     name VARCHAR(255),
--     location VARCHAR(255),
--     establishedDate DATE
-- );

-- SELECT 
--     name AS zooName,
--     location
-- FROM 
--     TIGER_ZOO_DB.PUBLIC.zoo;


-- 2. Достъп до данни вложен обект
-- Да се получат името (name) и видът (species) на директора на зоологическата градина. Целта е да се демонстрира как се извлича специфична информация от по-сложни структури данни, където данните са организирани йерархично (вложени). В случая, това са детайли за ключова фигура в управлението на зоопарка.

SELECT
    json_raw_data:director.name::STRING AS director_name,
    json_raw_data:director.species::STRING AS director_species
FROM TIGER_ZOO_DB.PUBLIC.raw_data;


-- 3. Изброяване на всички същества
-- Да се изброят името (name) и видът (species) на всяко същество в зоологическата градина. Тази задача цели да се създаде пълен каталог на всички животни, показващ основните им идентификатори. Това е полезно за инвентаризация, общи прегледи или като основа за по-сложни филтрирания.

-- Подсказка: Ще трябва да "сплеснете" (обработите като плосък списък) масива creatures.

SELECT
    creature.value:name::STRING AS name,
    creature.value:species::STRING AS species
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:creatures) AS creature;


-- 4. Филтриране на същества по произход
-- Да се намерят имената на всички същества, произхождащи от планетата 'Xylar'. Задачата изисква филтриране на данните по специфичен критерий – произход. Това позволява да се идентифицират групи същества с общи характеристики, което може да е важно за научни изследвания, образователни програми или специализирани грижи, свързани с техния роден свят.

SELECT
    creature.value:name::STRING AS name
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:creatures) AS creature
WHERE creature.value:originPlanet::STRING = 'Xylar';


-- 5. Заявка за хабитати по размер
-- Да се изброят имената и типовете на средата (environment types) на хабитати, по-големи от 2000 квадратни метра. Тази задача цели да се идентифицират най-просторните хабитати в зоопарка. Тази информация може да бъде полезна при планиране на разширения, разпределяне на нови видове, оценка на капацитета или за идентифициране на хабитати, подходящи за големи животни.

-- Подсказка: Не забравяйте да преобразувате размера в число за сравнение.


SELECT
    habitat.value:name::STRING AS habitat_name,
    habitat.value:environmentType::STRING AS environment_type,
    habitat.value:sizeSqMeters::NUMBER AS size_sq_meters
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:habitats) AS habitat
WHERE habitat.value:sizeSqMeters::NUMBER > 2000;


-- 6. Намиране на същества със специфични способности
-- Да се намерят имената на същества, които притежават специалната способност 'Camouflage' (Камуфлаж). Целта е да се търсят същества въз основа на техните уникални умения. Това може да е важно за образователни демонстрации, изследователски проекти относно адаптациите на видовете или за специфични програми за обогатяване на средата им.

-- Подсказка: Проверете дали масивът specialAbilities съдържа стойността. ARRAY_CONTAINS може да е полезен.

SELECT
    creature.value:name::STRING AS name
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:creatures) AS creature
WHERE ARRAY_CONTAINS('Camouflage'::VARIANT, creature.value:specialAbilities);

-- 7. Проверка на здравния статус на съществата
-- Да се изброят имената и здравният статус (полето status вътре в обекта healthStatus) на всички същества, чийто статус НЕ е 'Excellent' (Отличен). Тази задача е от критично значение за управлението на здравето на животните. Целта е бързо да се идентифицират всички същества, които може да се нуждаят от ветеринарна помощ, специални грижи или наблюдение, като се филтрират тези, които не са в перфектно здраве.

SELECT
    creature.value:name::STRING AS name,
    creature.value:healthStatus.status::STRING AS health_status
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:creatures) AS creature
WHERE creature.value:healthStatus.status::STRING <> 'Excellent';


-- 8. Разпределения на персонала**
-- Да се намерят имената и ролите на служителите, назначени към хабитат 'H001' (Crystal Caves - Кристалните пещери). Целта е да се получи информация за отговорностите на персонала, конкретно кои служители и с какви длъжности са ангажирани с поддръжката и грижата за определен хабитат. Това е важно за оперативното управление, координацията и при спешни случаи.

-- Подсказка: "Сплеснете" масива на персонала, след което проверете техния масив assignedHabitatIds.

SELECT
    staff_member.value:name::STRING AS employee_name,
    staff_member.value:role::STRING AS role
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:staff) AS staff_member
WHERE ARRAY_CONTAINS('H001'::VARIANT, CAST(staff_member.value:assignedHabitatIds AS ARRAY));


-- 9. Брой същества по хабитат**
-- Да се преброи колко същества пребивават във всеки habitatId. Да се покажат habitatId и броят. Тази задача предоставя обобщена информация за натовареността или гъстотата на популацията във всеки хабитат. Това е полезно за управление на ресурсите, планиране на пространството и осигуряване на подходящи условия за животните във всеки хабитат.



SELECT
    creature.value:habitatId::STRING AS habitat_id,
    COUNT(*) AS creature_count
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:creatures) AS creature
GROUP BY creature.value:habitatId::STRING
ORDER BY creature_count DESC;


-- 10. Изследване на характеристиките на хабитатите**
-- Да се изброят всички уникални характеристики, налични във всички хабитати. Целта е да се получи пълен списък на разнообразните елементи и характеристики (напр. водни басейни, катерушки, укрития), с които разполагат хабитатите в зоопарка. Това помага за оценка на общото обогатяване на средата, идентифициране на липсващи елементи или планиране на подобрения в хабитатите.

-- Подсказка: Първо ще трябва да "сплеснете" масива habitats, а след това да "сплеснете" масива features във всеки хабитат. Използвайте DISTINCT.

SELECT DISTINCT
    feature.value::STRING AS feature
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:habitats) AS habitat,
     LATERAL FLATTEN(input => habitat.value:features) AS feature
ORDER BY feature;


-- 11. Детайли за предстоящи събития
-- Да се извлекат името, типът и планираното време (като времеви печат - timestamp) за всички предстоящи събития. Тази задача е насочена към получаване на информация за планираните дейности и събития в зоопарка. Това е важно за информиране на посетителите, координация на персонала и логистично планиране на събитията.

SELECT
    event.value:name::STRING AS event_name,
    event.value:type::STRING AS event_type,
    TO_TIMESTAMP(event.value:scheduledTime::STRING) AS scheduled_time
FROM TIGER_ZOO_DB.PUBLIC.raw_data,
     LATERAL FLATTEN(input => json_raw_data:upcomingEvents) AS event
ORDER BY scheduled_time;

