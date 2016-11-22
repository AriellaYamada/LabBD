Exercício 1
---------------------------------------------------
INSTRUÇÕES PARA EXECUÇÃO DO GERADOR DE SCRIPT

1. Importar para o NetBeans o projeto Aula10Pt1
2. Incluir a biblioteca ojdbc14.jar
3. Executar o programa no NetBeans
4. Entrar com o nome da tabela que deseja gerar o script
5. Inserir as tags para indicar se a tabela precisa conter links, embedded ou nn
  ex: LE02CIDADE --embedded LE01ESTADO
      LE04BAIRRO --reference
      LE10CANDIDATURA --nn

Exercício 2
----------------------------------------------------

1. Executar o programa no NetBeans da mesma forma que no exercício anterior
2. Para gerar os indices utilizar a tag --indice logo após o nome da tabela

Exercício 3
---------------------------------------------------
1. Copiar cada grupo de check e colar no mongo shell

Exercício 4
---------------------------------------------------
1. Ter instalado o QT5, python3, mongodb, pymongo e PyQt5
2. Executar o wizard.py usando: python wizard.py
3. Para realizar as buscas, deve-se escolher uma tabela, e então montar a query, quando tudo estiver pronto, clicar em buscar.

Exercício 5
----------------------------------------------------
1. Importar para o NetBeans o projeto MongoTeste
2. Incluir as bibliotecas: bson-3.0.1.jar, mongodb-driver-3.0.1.jar e mongodb-driver-core-3.0.1.jar
3. O atributo de classe TAMANHO, controla a quantidade de inserts e de querys que terão
4. Na linha 50 do código, deve-se descomentar a linha caso queira rodar o teste usando indices.
