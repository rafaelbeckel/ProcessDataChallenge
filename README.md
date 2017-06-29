## Instalação
```bash
virtualenv -p python3 venv
. venv/bin/activate
pip install -e .
```
Depende do MongoDB 3.4 ou superior.


## Configuração
Editar variáveis no arquivo **settings.py** ou passar opções via commandline.


## Execução
`datadragon generate` Gera os dados

`datadragon reset` Apaga todos os documentos de todas as coleções

`datadragon crunch` Gera a tabela activities (ver formato abaixo)

`datadragon find "{...}"` Consulta a tabela activities

`datadragon --help` Lista os comandos disponíveis

`datadragon [COMMAND] --help` Lista opções disponíveis para o comando


## Observações

Imagine que você viva num mundo bizarro em que o produto "Lorem Ipsum" pode ser 
um livro, um eletrodoméstico e um game ao mesmo tempo. Criamos o datadragon para 
analisar os dados de compra desses produtos cósmicos da sétima dimensão.


### Comando generate

Gera produtos, usuários, carrinhos e pedidos fictícios.

O script usa todos os cores da máquina para otimizar a geração dos dados. A classe
**Seeder** delega o trabalho para os **Workers**, que rodam em paralelo (um em cada core).

Cada **Model** em data/models tem um método `generator(num)` que é usado pelos Workers 
para criar uma lista de documentos que serão inseridos no banco com `insert_many()`.

Depois de gerar os produtos, o seeder cria uma tabela intermediária **"drafts"** 
que junta dados de clientes com carrinhos/pedidos e então usa o próprio Mongo 
para gerar as tabelas user, carts e orders com referências reais uma para a outra.

O script está configurado para 800 usuários e um máximo de 6 pedidos por usuário,
o que pode gerar até 4800 pedidos. Com 8 cores, a geração das collections leva 
entre 30s e 1 minuto para essa quantidade.


### Comando crunch

Analisa os carrinhos e pedidos dos usuários e gera uma nova coleção chamada 'activity'.

O script usa o Aggregation Framework nativo do MongoDB para gerar a nova tabela
com a estrutura especificada abaixo em "Estrutura da nova tabela".

O mais eficiente seria gerar as tabelas com um formato mais otimizado direto no 
seeder, mas eu queria que o seeder simplesmente *modelasse* o problema exatamente 
como foi anunciado; e a responsabilidade do *cruncher* seria resolver o problema.


### Comando find

Delega uma query para o comando `find` do MongoDB na colação `activity`.


## Estrutura da Nova Tabela
```
{
    "_id" : ObjectId("595523a34f24116035bf5c71"),
    "customer_id" : ObjectId("595523a24f24116035bf5c6d"),
    "email" : "joao07@teixeira.org",
    "full_name" : "Francisco Castro",
    "average_monthly_expenses" : 476.68
    "monthly_expenses" : [
        {
            "year" : 2017,
            "month" : 2,
            "amount" : 586.07
        },
        {
            "year" : 2017,
            "month" : 4,
            "amount" : 1430.06
        },
        {
            "year" : 2016,
            "month" : 12,
            "amount" : 1009.49
        }
    ],
    "categorized_monthly_expenses" : [
        {
                "category" : "Móveis e Decoração",
                "year" : 2017,
                "month" : 2,
                "amount" : 5.96
        },
        {
                "category" : "Viagens",
                "year" : 2017,
                "month" : 4,
                "amount" : 698.3
        },
        {
                "category" : "Games",
                "year" : 2016,
                "month" : 12,
                "amount" : 151.02
        },
        
        ...
        
    ],
    "categorized_average_monthly_expenses" : [
        {
                "category" : "Móveis e Decoração",
                "amount" : 232.76
        },
        {
                "category" : "Games",
                "amount" : 232.76
        },
        {
                "category" : "Filmes, Séries e Música",
                "amount" : 243.92
        },
        {
                "category" : "Viagens",
                "amount" : 232.76
        }
    ],
}
```


## Estrutura das outras tabelas 

A estrutura das tabelas anunciadas foi simplificada, mantendo apenas os campos 
que eram relevantes para o escopo do teste

#### Tabela de usuários
```
{
    "_id" : ObjectId("58dec91ecd2f5b663934de21"),
    "customer_id" : "58d549d3808c3c0b89263d51",
    "email" : "joselito@aol.com",
    "details" : {
        "first_name" : "joselito",
        "last_name" : "mesquita",
        "full_name" : null
    },
}
```

#### Tabela de pedidos
```
{
    "_id" : ObjectId("58812ee70afc660de522c097"),
    "customer_id" : "58812e310afc660dbe1d0982",
    "cart_id" : "58812eaa0afc660dc06a2278",
    "created_at" : ISODate("2017-01-19T21:25:59.000Z")
}
```

#### Tabela de carrinhos
``` 
{
    "_id" : ObjectId("592872ad808c3c2a1d55204c"),
    "customer_id" : "59286c5b9d1b502aa37210a4",
    "products" : [ 
        {
            "product_id" : "584c477ccd2f5b78d554dfbb",
            "price" : 269.9,
            "quantity" : 1.0
        },
        {
            "product_id" : "584c477ccd2f5b78d554dfb1",
            "price" : 268.9,
            "quantity" : 3.0
        }
    ],
    "amount" : 269.9,
}
```

#### Tabela de produtos
```
{
    "_id" : ObjectId("584c4753cd2f5b713808e334"),
    "title" : "Ionto Vita Slim",
    "description" : "Eletrocosmético para tratamento de gordura localizada, principalmente masculina e de mulheres acima de 40 anos, e celulite, devido a sua altíssima concentração de ativos (34,5% de ativos) e sinergia com o ativo especializado Pheoslim. Indicado como auxiliar em procedimentos estéticos. Sua formulação contém algas e extratos vegetais, que são ricos em flavonoides, oligoelementos, saponinas, nucleoproteínas, lecitina, mucina. Potencializa os resultados se usado como base em aplicação de faixa gessada, pelo seu efeito oclusivo, ou após a aplicação de máscaras de argila, termoterapia ou crioterapia. Ótimo para ser associado à vinhoterapia e destoxi-redução.",
    "categories" : [ 
        "Cosméticos", 
        "Cosméticos Profissionais", 
        "Líquidos", 
        "Gordura Localizada"
    ],
    "price" : 152.37,
}
```
