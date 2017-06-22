import settings
from faker import Faker

def generate(num):
    fake = Faker(settings.FAKER_LANGUAGE)

    for i in range(num):
        first_name = fake.first_name()
        last_name = fake.last_name()

        user = {
            "customer_id" : fake.random_int(),
            "email" : fake.email(),
            "details" : {
                "first_name" : first_name,
                "last_name" : last_name,
                "full_name" : first_name + ' ' + last_name,
                "birthdate" : "ISODate("+fake.iso8601()+")",
                "age_group" : "",
                "telephone" : fake.phone_number(),
                "identification" : [ 
                    {
                        "type" : "cpf",
                        "typeName" : "CPF",
                        "number" : "00000000000"
                    }
                ]
            },
            "address" : [
                {
                    "address" : fake.street_name(),
                    "number" : fake.building_number(),
                    "address_complement" : "",
                    "city" : fake.city(),
                    "state" : fake.estado_sigla(),
                    "country" : fake.country(),
                    "zipcode" : fake.postcode(),
                    "is_primary" : "true"
                }
            ],
            "updated_at" : "ISODate("+fake.iso8601()+")",
            "created_at" : "ISODate("+fake.iso8601()+")"
        }

        yield user