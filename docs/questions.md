
* soll deadline relativ (done in 2 studen) oder absolut (done am 17. Nov 13:54) sein.
* 



This to do:

reservation structure (wie suche ich darin einfach, update die available memory, cpu etc? Update ich das überhaupt?)
Wie mach ich requests.
Wie sehen eine Pläne aus?
Wie checke ich ob die Requests überhaupt gehen. Request gibt CPU aufwand schon an oder? Sprich haben wir im Vorhersage-Modell.
Wie wo in welcher Form hab ich die Vorhersagemodelle?



Machines 
  |
  Machine (memory shared)
     |
    Node (memory)
      |
    Core 


Nodes and CPU Cores are equal in the Phd thesis so no need for both. 


Sollte ich es lieber in C machen da die Pointer Sachen einfacher sind??


1. Quantitative partitioning: Which program obtains how many processors?
2. Qualitative partitioning: Which program obtains which processors?
3. Clustering (contraction) within the programs: Which threads are grouped
together?
4. Injective allocation (mapping): Which thread group is mapped to which pro-
cessor?


Machine -> Nodes (shared speicher) -> Cores

Mappe auf Cores

Gibt nur eine Machine


bh_mod_timing; calculation real cpu_time für task; runtime is für job


Passen Processe 1. passen hintereinadner? dann nice sonst nächsten process zum Nachbar


Gesamt Performance checken ob ich genug hab. Summiere Speicher.Auch wenn nur 80 % von was ich brauch probiere trozdem
Dann gleiches mit Prozess, wenn ich es 10 Mal umsotiere aber es geht nicht auch weg, verusche es nicht weiter

comm tasks ignoriere ich. 

Modell mit allen Jobs, Prozesse, task modell (Is task communication oder calculation, runtime und miene memory consumption)



1. Spalte - Task-Nr.
2. Spalte - Prozess-Nr.
3. Spalte - Task-Typ (0 = Start, 1 = End, 2 = Fork, 10 = Fork End, 20 = Join, 4 = 
Calculation, 5 = Communication)
4. - Speicherverbrauch
Rest ist Verweis auf nächsten Task im Prozessablauf

4-5 Anzahl der Bytes die Übungertragen werden.



Neue Frage:
* CPUs können unterschiedliche Taktfrequenzen haben. Können die Taktfrequenzen zwischen Cores auch unterschiedlich sein? - Gehe aktuell davon aus dass der die Taktfrequenze innerhalb eines Nodes gleich ist.

* Wie ist die Zeiteinheit in der Reservierung. Eine Taktlänge basiert ja auf der Frequenz des Nodes. Wann genau startet eine für das Modell oder wird es abstrahiert. 



CPU Zeiten. Instruktion in Richtung Zeiten
Rüstzeit Zeit die benötigt wird um alle Voraussetzung des Arbeitspackets vorzuubereiten
Rüstzeiten kann ich vernachlässen und in Future Work thematisieren. Kann dann als Puffer reserviert werden. 

Gerne Unterkapitel in Future Work als was man alles beachten sollte um es in der realität einzusetzten.
Fragen:
* Haben Comm Task immer die gleiche Anzahl and Instructions? Kann ich bei Fork, ForkEnd und Join Tasks eine Instruktion angeben oder sollte es eher null sein?

Challange: I did not think about the fact that a process allocates in the beginning and frees at the end and does not free it inbetween 


Für Masterarbeit: Bei Comm Task eigentlich keine Memory nötig (außer Puffer)


New Questions:
* test mapping of processes when one process is the parent of the currently mapped one do I have to think about the fact that the parent process will have a p_res that takes up memory until the last forked process of this chain is mapped or can I be less strict and concentrate on the approx_runtime and just check that all process_res are vefore the deadline is?