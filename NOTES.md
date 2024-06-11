# Sqlite something
Started as a codecrafters project but I'm on the free tier so I don't have access to stages after 2 whoops
# Design notes ...?
I was thinking of using a command pattern for queries (for now I'm not interested in insert and update)
This way I can have different strategies for querying data but implementation of later ones should be more straightforward

## Design...decisions...?
Let's break down the steps necessary for a "simple" query:
SELECT id, nome, cognome from dipendenti WHERE anno_di_nascita >= 2000;

Prima di tutto parso la query  e ottengo un SelectStatement
Poi uso il visitor che chiamerò Query o qualcosa di simile:
Primo step -> entro nel from e vado a prendermi il nome della tabella interessata
   con il nome della tabella eseguo una "subquery" : SELECT * from sqlite_schema WHERE name = nome_tabella (funziona esattamente come query normale)
   quindi l'output di visit_from sarà una definizione di una tabella + una rowpage (sqlite_schema sarà statica)
   ora vado nel where con ast della tabella e controllo che l'espressione abbia senso (oioioi) e da quella ci creo una sorta di lambda da eseguire poi nel rowscan
   quindi visit_where mi sputa una specie di lambda da eseguire su ogni riga
   uscito da from e where uso ast di create, rowpage e filtro per andare a prendere le righe che mi interessano
   infine aggrego le righe interessate in un qualche output(una mappa? un semplice vettore di vettori ben ordinato?)
   fine :)
IT FUCKING WORKS
Ora ci sta un bel po' di pulizia da fare, ma le select funzionano

# TODO:
- [ ] Supportare funzioni e funzioni di aggregazione nelle query, le prime sono "semplici" keywords che chiamano qualcosa di scritto a manovella da me
le altre invece richiedono qualche accortezza in più (e forse anche la gestione del group by che brrr)
- [ ] Estrarre il visitor in modo che non sia dbaccess a implementarlo ma dbaccess gli sia passato come arg
- [ ] Implementare visitor per lettura delle Create table che per ora è solo na roba pezzotta
- [ ] Pensare se ci siano metodi migliori (probabile) di gestire le expressions
- [ ] Implementare LIKE come operatore per le espressioni
- [ ] Ora come ora non mi sono molto concentrato sulla case insensitiveness che invece contraddistingue sql
- [ ] Testare testare testere
- [ ] Astrazione delle funzionalità di output in modo ad esempio da avere output in json o in txt o in csv a mio piacere
- [ ] Creare un query plan in qualche modo -> quindi invece di un singolo visitor che faccia tutto quello che vuole voglio più visitor da comporre per creare una query, anzi piuttosto che più visitor meglio un visitor che genera dei QueryPlan: trait che prendono un provider (DbAccess) e un set di dati in input e lo elaborano per creare l'output
- [ ] Probabilmente nell'ottica di inserire operazioni come le query conviente anche modificare come vengono effettuati i filtri: filtrare mentre si legge è efficiente ma non puoi fare le join

> NOTA MOLTO IMPORTANTE
> un visitor separato mi permetterebbe semi elegantemente di gestire cose come le funzioni di aggregazione, in quanto 
> può essere lui a gestire lo stato di cose come count e la funzione a quel punto semplicemente sarebbe un callback a count++ 
> circa perché ho già visto che precompilare con lo stato è molto dangerous 

# Urge rework in ottica di query plan
Ho bisogno di un trait QueryPlan, che prende una full table (amen, poi vedrò se si può migliorare qualcosa in termini di performance, 
tipo una SimpleQuery, ovvero niente join o aggregazioni strane e ce quindi posso eseguire direttamente in fase di lettura). Poi di sicuro 
