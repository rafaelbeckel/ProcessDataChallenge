## Instalação
`virtualenv -p python3 envname`
`. venv/bin/activate`
`pip install -e .`


## Configuração
Editar variáveis no arquivo settings.py ou passar opções via commandline.

O script está configurado para 800 usuários e um máximo de 6 pedidos por usuário,
o que pode gerar até 4800 pedidos. Com 8 cores, a execução leva entre 30s e 2 minutos 
para essa quantidade, dependendo de quantos produtos os "usuários" comprem.


## Execução
`datadragon generate` Gera os dados

`datadragon reset` Apaga todos os documentos de todas as coleções

`datadragon crunch` (não foi implementado) Gera a tabela nova

`datadragon find "{...}"` (não foi implementado) Consulta a tabela nova

`datadragon --help` Lista os comandos disponíveis

`datadragon [COMMAND] --help` Lista opções disponíveis para o comando


## Sobre minha experiëncia com o teste
Me concentrei mais na geração do dataset do que na consulta.

O script usa todos os cores da máquina para otimizar a geração dos dados. A classe
Seeder delega o trabalho para os Workers, que rodam em paralelo (um em cada core).

Cada Model em data/models tem um método "generator(num)" que é usado pelos Workers 
para criar uma lista de documentos que serão inseridas no banco com insert_many().

Na hora de gerar as tabelas de carrinho e pedidos, encontrei um problema: elas
precisam fazer referência às outras tabelas. Isso estava sendo feito no próprio
generator() delas, o que rodava uma consulta dentro de cada loop. Funciona, mas 
não é eficiente para uma grande quantidade de dados.

Gerar carts e orders aleatórios sem fazer referências iria tornar a tabela nova 
inútil. Então tentei uma abordagem diferente para o mesmo problema:

Depois de gerar os produtos, o seeder cria uma tabela intermediária "activity" 
que junta dados de clientes com carrinhos/pedidos e então usa o próprio Mongo 
para gerar as tabelas separadas com referências reais uma para a outra.

Essa modificação reduziu o tempo de geração das tabelas em 3x.


## Aggregation framework
Embora não tenha finalizado a tempo o enunciado do teste, imagino que a abordagem 
esperada seria algo usando o agreggation framework do Mongo, mais ou menos assim:

```python3
db.aggregate({
    '$project' : {
        ... # (estrutura da nova tabela)
    },
    {'$out' : 'nova_tabela'}
})
```

Essa técnica é usada no seeder para transformar a tabela temporária "activity"
nas tabelas "user", "carts" e "orders" (linhas 77 a 127). 


#### Estrutura da Nova Tabela
```
{
    “_id” : “XXXXX”,
    "customer_id" : "58d549d3808c3c0b89263d51",
    "email" : "joselito@aol.com",
    "details" : {
        "first_name" : "joselito",
        "last_name" : "mesquita”
    },
    “monthly_spenses”: {
        “jan” : 123.45,
        “fev” : 678.90,
        “mar” : 0, 
        …
    }, 
    “categorized_monthly_expenses” : {
        “jan” : {  
            “categoria_1” : 123.00,
            “categoria_2” : 0.45
        },    
        …
    },
    “monthly_avg_expense” : 345.67,
    “categorized_monthly_avg_expense” : {
        “categoria_1” : 123.00,
        “categoria_2” : 0.45
        …
    }
```


## Estrutura das outras tabelas 
A estrutura das tabelas foi simplificada, mantendo apenas os campos que eram 
relevantes para o escopo do teste

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