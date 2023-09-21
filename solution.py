from dataclasses import dataclass

import lxml.html as l
import pandas as pd

from crawler import Crawler

http = open('proxy/http.txt').read().splitlines()

socks4 = open('proxy/socks4.txt').read().splitlines()

socks5 = open('proxy/socks5.txt').read().splitlines()

proxies_list = list(map(lambda x: "http://" + x, http))
proxies_list.extend(map(lambda x: "socks4://" + x, socks4))
proxies_list.extend(map(lambda x: "socks5://" + x, socks5))


@dataclass
class ParseContext:
    name: str = "undefined"
    body_type: str = "undefined"
    modification_name: str = "undefined"

    price_min: int = -1
    price_max: int = -1

    country: str = "undefined"
    classe: str = "undefined"

    doors_count: int = -1
    seats_count: int = -1
    steering_wheel_side: str = "undefined"

    front_suspension_type: str = "undefined"
    rear_suspension_type: str = "undefined"
    front_brakes_type: str = "undefined"
    rear_brakes_type: str = "undefined"

    length: int = -1
    width: int = -1
    height: int = -1
    wheelbase: int = -1
    clearance: int = -1
    front_track: int = -1
    rear_track: int = -1

    max_speed: int = -1
    zero_to_100: float = float('NaN')
    fuel_consumption_city: float = float('NaN')
    fuel_consumption_road: float = float('NaN')
    fuel_consumption_mixed: float = float('NaN')
    fuel_type: str = "undefined"
    emission_class: str = "undefined"

    trunk_volume_min: int = -1
    trunk_volume_max: int = -1
    fuel_tank_volume: int = -1
    equipped_weight: int = -1
    max_weight: int = -1

    gearbox_type: str = "undefined"
    gear_count: int = -1
    drive_type: str = "undefined"

    engine_type: str = "undefined"
    engine_position: str = "undefined"
    engine_rotation: str = "undefined"
    engine_displacement: int = -1
    engine_turbo_type: str = "undefined"
    engine_max_horsepower: int = -1
    engine_max_power_kilowatt: int = -1
    engine_max_power_rpm: int = -1
    engine_max_torque: int = -1
    engine_max_torque_rpm: int = -1
    engine_cylinders_position: str = "undefined"
    engine_cylinders_count: int = -1
    engine_valves_per_cylinder: int = -1
    engine_intake_type: str = "undefined"
    engine_compression: float = float('NaN')
    engine_cylinder_diameter: float = float('NaN')
    engine_piston_stroke: float = float('NaN')


parsed_data_names = [
    "Название",
    "Тип кузова",
    "Название модификации",

    "Цена, мин",
    "Цена, макс",

    "Страна марки",
    "Класс автомобиля",
    "Количество дверей",
    "Количество мест",
    "Расположение руля",

    "Тип передней подвески",
    "Тип задней подвески",
    "Передние тормоза",
    "Задние тормоза",

    "Длина",
    "Ширина",
    "Высота",
    "Колёсная база",
    "Клиренс",
    "Ширина передней колеи",
    "Ширина задней колеи",

    "Максимальная скорость",
    "Разгон до 100 км/ч",
    "Расход топлива в городе",
    "Расход топлива по трассе",
    "Расход топлива смешанный",
    "Марка топлива",
    "Экологический класс",

    "Объём багажника минимальный",
    "Объём багажника максимальный",
    "Объём топливного бака",
    "Снаряженная масса",
    "Полная масса",

    "Коробка передач",
    "Количество передач",
    "Тип привода",

    "Тип двигателя",
    "Расположение двигателя",
    "Положение двигателя (рысканье)",
    "Объём двигателя",
    "Тип наддува",
    "Максимальная мощность, л.с.",
    "Максимальная мощность, кВт.",
    "Обороты достижения максимальной мощности",
    "Максимальный крутящий момент, н*м",
    "Обороты достижения максимального крутящего момента",
    "Расположение цилиндров",
    "Количество цилиндров",
    "Число клапанов на цилиндр",
    "Система питания двигателя",
    "Степень сжатия",
    "Диаметр цилиндра",
    "Ход поршня",
]


def parser(self: Crawler, depth, context, body: bytes):
    if depth == 0:
        page = l.fromstring(body.decode('utf-8'))
        models = page.xpath("//a[@class='Link CatalogListing__itemLink-yu3b6']")
        print(f"Got {len(models)} models, enqueuing")
        for model in models:
            link = model.attrib['href']
            self.enqueue(link, 1)
            # break # For debugging
    elif depth == 1:
        page = l.fromstring(body.decode('utf-8'))
        tab = page.xpath("//a[@class='Tabs__link']")[0].attrib['href']
        print(f"Got 'Характеристики' link: {tab}, enqueuing")
        self.enqueue(tab, 2)
    elif depth == 2:
        page = l.fromstring(body.decode('utf-8'))
        models = page.xpath("//div[@class='SpecificationContent__configuration']")
        print(f"Got {len(models)} generations")
        for model in models:
            link = model.xpath("./div/a")[0].attrib['href']
            print(f"Got 'Все характеристики' link: {link}, enqueuing")
            self.enqueue(link, 3)
            # break # For debugging
    elif depth == 3:
        page = l.fromstring(body.decode('utf-8'))
        modifications = page.xpath("//a[@class='Link ModificationsItem__link-iGtp0']")
        print(f"Got {len(modifications)} modifications")
        for i in modifications:
            link = i.attrib['href']
            self.enqueue(link, 4)
            # break # For debugging
    elif depth == 4:
        page = l.fromstring(body.decode('utf-8'))
        context = ParseContext()

        try:
            # Название
            name_holder = page.xpath("//h1[@class='CatalogFilterForm__title-G2dm6']")[0].text
            sub_name = name_holder.lstrip("Модели").split(',')
            body_type = sub_name[-1]
            name = ",".join(sub_name[:-1])
            context.name = name.strip()
            context.body_type = body_type.strip()
        except IndexError:
            pass

        # Тип кузова

        try:
            # Название модификации
            modification_holder = page.xpath("//h2[@class='ModificationHeader__title-UuoLw']")[0].text
            context.modification_name = modification_holder.lstrip("Модификация").strip()
        except IndexError:
            pass

        try:
            # Цена, мин
            price_holder = page.xpath("//a[@class='Link ModificationHeader__priceLink-rqA66']")[0].text
            context.price_min = int(price_holder.split('–')[0].strip().replace("\xa0", ''))
            # Цена, макс
            context.price_max = int(price_holder.split('–')[1].rstrip("₽").strip().replace("\xa0", ''))
        except (IndexError, ValueError):
            pass

        properties = page.xpath("//li[@class='ModificationInfo__option-UwXWB']")

        for holder in properties:
            try:
                match holder.xpath("span[@class='ModificationInfo__optionName-iuJYq']")[0].text.strip():
                    case "Страна марки":
                        context.country = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Класс автомобиля":
                        context.classe = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Количество дверей":
                        context.doors_count = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.strip())
                    case "Количество мест":
                        seats_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                        if ',' in seats_holder:
                            context.seats_count = int(seats_holder.split(',')[0].strip())
                        else:
                            context.seats_count = int(seats_holder)
                    case "Расположение руля":
                        context.steering_wheel_side = \
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                                0].text.strip()
                    case "Длина":
                        context.length = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "мм").strip())
                    case "Ширина":
                        context.width = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "мм").strip())
                    case "Высота":
                        context.height = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "мм").strip())
                    case "Колёсная база":
                        context.wheelbase = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "мм").strip())
                    case "Клиренс":
                        clearance_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.rstrip("мм").strip()
                        if '-' in clearance_holder:
                            context.clearance = int(clearance_holder.split('-')[0].strip())
                        else:
                            context.clearance = int(clearance_holder)
                    case "Ширина передней колеи":
                        context.front_track = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "мм").strip())
                    case "Ширина задней колеи":
                        context.rear_track = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "мм").strip())
                    case "Объем багажника мин/макс":
                        trunk_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.rstrip(
                            "л").strip().split('/')
                        context.trunk_volume_min = int(trunk_holder[0].strip())
                        if len(trunk_holder) > 1:
                            context.trunk_volume_max = int(trunk_holder[1].strip())
                        else:
                            context.trunk_volume_max = context.trunk_volume_min
                    case "Объём топливного бака":
                        context.fuel_tank_volume = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "л").strip())
                    case "Снаряженная масса":
                        context.equipped_weight = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "кг").strip())
                    case "Полная масса":
                        context.max_weight = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "кг").strip())
                    case "Коробка передач":
                        context.gearbox_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Количество передач":
                        context.gear_count = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.strip())
                    case "Тип привода":
                        context.drive_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Тип передней подвески":
                        context.front_suspension_type = \
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                                0].text.strip()
                    case "Тип задней подвески":
                        context.rear_suspension_type = \
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                                0].text.strip()
                    case "Передние тормоза":
                        context.front_brakes_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Задние тормоза":
                        context.rear_brakes_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Максимальная скорость":
                        context.max_speed = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "км/ч").strip())
                    case "Разгон до 100 км/ч":
                        context.zero_to_100 = float(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "с").strip())
                    case "Расход топлива, город/трасса/смешанный":
                        consumption_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.rstrip("л/100 км").strip().split('/')
                        try:
                            context.fuel_consumption_city = float(consumption_holder[0].strip())
                        except ValueError:
                            pass
                        if len(consumption_holder) > 1:
                            try:
                                context.fuel_consumption_road = float(consumption_holder[1].strip())
                            except ValueError:
                                pass
                        else:
                            context.fuel_consumption_road = context.fuel_consumption_city
                        if len(consumption_holder) > 2:
                            try:
                                context.fuel_consumption_mixed = float(consumption_holder[2].strip())
                            except ValueError:
                                pass
                        else:
                            try:
                                context.fuel_consumption_mixed = (
                                                                         context.fuel_consumption_city + context.fuel_consumption_road) / 2
                            except Exception:
                                pass
                    case "Марка топлива":
                        context.fuel_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Экологический класс":
                        context.emission_class = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Тип двигателя":
                        context.engine_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Расположение двигателя":
                        engine_pos_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip().split(',')
                        context.engine_position = engine_pos_holder[0].strip()
                        if len(engine_pos_holder) > 1:
                            context.engine_rotation = engine_pos_holder[1].strip()
                    case "Объем двигателя":
                        context.engine_displacement = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.rstrip(
                                "см³").strip())
                    case "Тип наддува":
                        context.engine_turbo_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Максимальная мощность":
                        engine_power_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.rstrip("об/мин").strip().split(' ')
                        engine_power_sub_holder = engine_power_holder[0].strip().split('/')
                        try:
                            context.engine_max_horsepower = int(engine_power_sub_holder[0].strip())
                        except ValueError:
                            pass
                        if len(engine_power_sub_holder) > 1:
                            try:
                                context.engine_max_power_kilowatt = int(engine_power_sub_holder[1].strip())
                            except ValueError:
                                pass
                        else:
                            try:
                                context.engine_max_power_kilowatt = context.engine_max_horsepower // 1.36
                            except Exception:
                                pass
                        if engine_power_holder[-1].strip().isnumeric():
                            try:
                                context.engine_max_power_rpm = int(engine_power_holder[-1].strip())
                            except ValueError:
                                pass
                    case "Максимальный крутящий момент":
                        engine_torque_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.rstrip("об/мин").strip().split(' ')
                        try:
                            context.engine_max_torque = int(engine_torque_holder[0].strip())
                        except ValueError:
                            pass
                        if engine_torque_holder[-1].strip().isnumeric():
                            context.engine_max_torque_rpm = int(engine_torque_holder[-1].strip())
                    case "Расположение цилиндров":
                        context.engine_cylinders_position = \
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.strip()
                    case "Количество цилиндров":
                        context.engine_cylinders_count = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.strip())
                    case "Число клапанов на цилиндр":
                        context.engine_valves_per_cylinder = int(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.strip())
                    case "Система питания двигателя":
                        context.engine_intake_type = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.strip()
                    case "Степень сжатия":
                        context.engine_compression = float(
                            holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[0].text.strip())
                    case "Диаметр цилиндра и ход поршня":
                        cylinder_size_holder = holder.xpath("span[@class='ModificationInfo__optionValue-V_utP']")[
                            0].text.rstrip("мм").strip().split('x')
                        try:
                            context.engine_cylinder_diameter = float(cylinder_size_holder[0].strip())
                        except ValueError:
                            pass
                        if len(cylinder_size_holder) > 1:
                            try:
                                context.engine_piston_stroke = float(cylinder_size_holder[1].strip())
                            except ValueError:
                                pass
                    case other:
                        print(f"Проигнорирован параметр '{other}'")
            except (IndexError, ValueError) as e:
                print(f"Exception {e}")
        self.result.append(context)
    else:
        raise NotImplementedError("Unknown page")


cookies = {
    "i": "bQAaEgRfI2dZ0DFKoGy9uRwQ1cZP4lVYyylkVvYJgVZGgLDg2WfM+hYF2MbCNsHsKeQd13K+92maaqz+FLE1lMyzvUY=",
    "yandexuid": "5841648601694695324",
    "L": "aElyZl0BY3hpa39CVGFjBlxYVFxiAHlTSiczGQ5cVg==.1694695396.15465.353863.d749ec239c6cf9db6274b8c26c237cc6",
    "sso_status": "sso.passport.yandex.ru:synchronized",
    "yp": "1695148618.yu.5841648601694695324",
    "ymex": "1697654218.oyu.5841648601694695324",
    "_csrf": "5Pn6uHlVBxhsf9154fsXOrtO",
    "desktop_session_key": "149d17c94e2725b74669b976b363eb7b7ee906940425c666ddd0f2edc9fccbaa5011b9b98e893edea9628f49c2be8500d99bcb5622eaf767eebefd6366d77a1eb258e2c7430241d2aaf29d9af2939fba724b82e0d38e6b62eab3010d80f6f780",
    "desktop_session_key.sig": "by3vC6e3UbzJhvggWBafU9jCyBQ",
    "_yasc": "Myxe0LzobkL74W275qN2WBlqGndwL1G6lIDpcDHFC+boLuL6oZLiiDkJsrOmgOm6",
    "PHPSESSID": "375c4f2570b9c7abaeb6bf2b396525dd",
    "user_country": "ru",
    "yandex_gid": "2",
    "ya_sess_id": "noauth:1695068078",
    "sessar": "1.1182.CiB7vuv_XWGsDvtWFp-h0YNw4e-sv4F8ZAVz6hlMjjWwfQ.IHkEXfOng9OSHItUriyUB0fkuHOhJXPrdEuk8epRFH0",
    "yandex_login": "",
    "ys": "c_chck.2187868790",
    "mda2_beacon": "1695068078220",
    "_ym_d": "1695068100",
    "cycada": "hUrmhEPJMcBkMCcnTliit+aMfYvPBkWZIUG2KUX+eCs=",
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'}

# parser(None, 4, None, open('data/test.html', mode="rb").read())
crawler = Crawler(parser, proxies_list, cookies, headers, [])
for i in range(1, 26):
    crawler.enqueue(f"https://auto.ru/catalog/cars/all/?page={i}")
crawler.join()

df = pd.DataFrame(crawler.result)

df['body_type'] = pd.Categorical(df['body_type'])
df['country'] = pd.Categorical(df['country'])
df['classe'] = pd.Categorical(df['classe'])
df['steering_wheel_side'] = pd.Categorical(df['steering_wheel_side'])
df['front_suspension_type'] = pd.Categorical(df['front_suspension_type'])
df['rear_suspension_type'] = pd.Categorical(df['rear_suspension_type'])
df['front_brakes_type'] = pd.Categorical(df['front_brakes_type'])
df['rear_brakes_type'] = pd.Categorical(df['rear_brakes_type'])
df['fuel_type'] = pd.Categorical(df['fuel_type'])
df['emission_class'] = pd.Categorical(df['emission_class'])
df['gearbox_type'] = pd.Categorical(df['gearbox_type'])
df['drive_type'] = pd.Categorical(df['drive_type'])
df['engine_type'] = pd.Categorical(df['engine_type'])
df['engine_position'] = pd.Categorical(df['engine_position'])
df['engine_rotation'] = pd.Categorical(df['engine_rotation'])
df['engine_turbo_type'] = pd.Categorical(df['engine_turbo_type'])
df['engine_cylinders_position'] = pd.Categorical(df['engine_cylinders_position'])
df['engine_intake_type'] = pd.Categorical(df['engine_intake_type'])

df.to_csv("data/result.csv")
