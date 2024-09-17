package io.github.rkolesnev.kstream.customstores.neo4j.dataproducer;

import io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.pojo.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

import static io.github.rkolesnev.kstream.customstores.neo4j.streamsapp.KStreamApp.NODES_INPUT_TOPIC;

public class PersonDataProducer {
    public static void main(String[] args) {
        Properties props = getProducerProperties();
        int personsNumber = 40;
        try (KafkaProducer<String, NodeInputPojo> producer = new KafkaProducer<>(props)) {
            getPersonsDataToSend(personsNumber).forEach(producer::send);
            producer.flush();
            getRelationshipsDataToSend(personsNumber).forEach(producer::send);
            producer.flush();
        }

    }

    private static List<ProducerRecord<String, NodeInputPojo>> getRelationshipsDataToSend(int numberOfRecords) {
        long seedNumRels = 123456L;
        long seedTargetUsers = 123456L;
        Random numRelsRandom = new Random(seedNumRels);
        Random targetUsersRandom = new Random(seedTargetUsers);
        List<NodeInputPojo> relationships = new ArrayList<>();
        List<Pair<String, String>> personsDataSlice = Arrays.asList(PERSONS_DATA).subList(0, numberOfRecords);

        for (Pair<String, String> person : personsDataSlice) {
            int numberOfPersonsToFollow = numRelsRandom.nextInt(5) + 1; //all nodes to have 1 to 5 relationship
            Set<String> targets = new HashSet<>(); //dont want duplicate relationships - track and eliminate dupes
            for (int i = 0; i < numberOfPersonsToFollow; i++) {
                int targetPersonId = targetUsersRandom.nextInt(numberOfRecords);
                String targetPerson = PERSONS_DATA[targetPersonId].getLeft();

                while (targetPerson.equals(person.getLeft()) || targets.contains(targetPerson)) { //Do not allow relationship to self
                    targetPersonId = targetUsersRandom.nextInt(numberOfRecords);
                    targetPerson = PERSONS_DATA[targetPersonId].getLeft();
                }
                targets.add(targetPerson);
                relationships.add(NodeInputPojo.builder().opType(NodeInputPojo.OpType.FOLLOW_PERSON).followPersonOp(new FollowPersonOp(person.getLeft(), targetPerson)).build());
            }
        }
        return relationships.stream().map(rel -> new ProducerRecord<>(NODES_INPUT_TOPIC, rel.getFollowPersonOp().getUsername(), rel)).toList();
    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, NodeInputJsonSerde.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "persons-producer-" + UUID.randomUUID());
        return props;
    }

    private static List<ProducerRecord<String, NodeInputPojo>> getPersonsDataToSend(int numberOfRecords) {
        List<Pair<String, String>> personsDataSlice = Arrays.asList(PERSONS_DATA).subList(0, numberOfRecords);
        return personsDataSlice.stream().map(person -> new ProducerRecord<>(NODES_INPUT_TOPIC,
                        person.getLeft(),
                        NodeInputPojo.builder()
                                .opType(NodeInputPojo.OpType.CREATE_PERSON)
                                .createPersonOp(new CreatePersonOp(new PersonPojo(person.getLeft(), person.getLeft() + " " + person.getRight())))
                                .build()))
                .toList();
    }

    public static Pair<String, String>[] PERSONS_DATA = new Pair[]{
            Pair.of("Nanette", "Kilshaw"),
            Pair.of("Jeffrey", "Allardyce"),
            Pair.of("Leeanne", "Gettens"),
            Pair.of("Alex", "Setchfield"),
            Pair.of("Karlen", "Gierhard"),
            Pair.of("Antoni", "Nurden"),
            Pair.of("Thomasina", "Bootland"),
            Pair.of("Lissie", "Ream"),
            Pair.of("Robinet", "Vahl"),
            Pair.of("Pru", "Collingridge"),
            Pair.of("Laurianne", "Greeno"),
            Pair.of("Dolores", "Atkins"),
            Pair.of("Lind", "Harly"),
            Pair.of("Corinna", "Follin"),
            Pair.of("Wald", "Pfeffel"),
            Pair.of("Vinny", "Vondra"),
            Pair.of("Erasmus", "Murcutt"),
            Pair.of("Merle", "Weekes"),
            Pair.of("Sheeree", "Chilley"),
            Pair.of("Vivian", "Ledwidge"),
            Pair.of("Charmion", "Ridder"),
            Pair.of("Kare", "Glaysher"),
            Pair.of("Christye", "Stanistreet"),
            Pair.of("Mellisent", "Pelling"),
            Pair.of("Marga", "Curbishley"),
            Pair.of("Zack", "Delia"),
            Pair.of("Arlin", "Smythin"),
            Pair.of("Adriena", "Opfer"),
            Pair.of("Pietrek", "Hassur"),
            Pair.of("Drucie", "Egel"),
            Pair.of("Augy", "Quare"),
            Pair.of("Roda", "Welton"),
            Pair.of("Dorine", "Meth"),
            Pair.of("Aida", "Alu"),
            Pair.of("Zahara", "Massow"),
            Pair.of("Fayina", "Baldocci"),
            Pair.of("Bentley", "Calvard"),
            Pair.of("Cathrine", "Cristofolini"),
            Pair.of("Raimund", "Chastaing"),
            Pair.of("Esmeralda", "Sturman"),
            Pair.of("Cherianne", "Werlock"),
            Pair.of("Conney", "Cardenoza"),
            Pair.of("Darlene", "Sturney"),
            Pair.of("Kippy", "Haggarth"),
            Pair.of("Barnabas", "Gifkins"),
            Pair.of("Saidee", "Gellibrand"),
            Pair.of("Myrle", "Brandham"),
            Pair.of("Hugues", "Roadknight"),
            Pair.of("Risa", "Rounsefell"),
            Pair.of("Pollyanna", "Sizland"),
            Pair.of("Babara", "Nutton"),
            Pair.of("Jorry", "Lewerenz"),
            Pair.of("Suzanne", "Matzel"),
            Pair.of("Kinna", "Rubens"),
            Pair.of("Konstantin", "Kempston"),
            Pair.of("Daryn", "Worton"),
            Pair.of("Carson", "Kamienski"),
            Pair.of("Kiley", "Kleynermans"),
            Pair.of("Glynn", "Charlin"),
            Pair.of("Lexine", "Cluitt"),
            Pair.of("Kelcy", "McMarquis"),
            Pair.of("Dagmar", "Gotthard.sf"),
            Pair.of("Wang", "Corbert"),
            Pair.of("Meagan", "MacFie"),
            Pair.of("Breena", "Jeanet"),
            Pair.of("Mitch", "Rosenbush"),
            Pair.of("Hansiain", "Conlaund"),
            Pair.of("Honor", "Brightman"),
            Pair.of("Ysabel", "Rois"),
            Pair.of("Emile", "Sleight"),
            Pair.of("Gloria", "Tesche"),
            Pair.of("Avis", "Ebbage"),
            Pair.of("Darcy", "Tuplin"),
            Pair.of("Eugine", "Baldacco"),
            Pair.of("Stanislaus", "Laherty"),
            Pair.of("Cindie", "Churching"),
            Pair.of("Morton", "Korda"),
            Pair.of("Dru", "Tzarkov"),
            Pair.of("Clemmie", "Cowland"),
            Pair.of("Dody", "Scini"),
            Pair.of("Bernardina", "Vasentsov"),
            Pair.of("Dave", "Hessel"),
            Pair.of("Kerby", "McCuis"),
            Pair.of("Harry", "Hubeaux"),
            Pair.of("Alys", "Crowden"),
            Pair.of("Kathlin", "Ferraresi"),
            Pair.of("Ammamaria", "Shrive"),
            Pair.of("Issie", "Chiommienti"),
            Pair.of("Venus", "Cantu"),
            Pair.of("Meghann", "Rogister"),
            Pair.of("Sheppard", "Cord"),
            Pair.of("Rossie", "Hazeldene"),
            Pair.of("Leonidas", "Tuny"),
            Pair.of("Fredra", "O'Scollee"),
            Pair.of("Vivienne", "Yeo"),
            Pair.of("Wheeler", "Tollemache"),
            Pair.of("Corbett", "Hinckesman"),
            Pair.of("Mella", "MacCart"),
            Pair.of("Karoly", "Deegan"),
            Pair.of("Leona", "Pavelka"),
            Pair.of("Birgit", "Poynor"),
            Pair.of("Edin", "Berthot"),
            Pair.of("Cullan", "Brabbins"),
            Pair.of("Gordan", "Pirrey"),
            Pair.of("Auroora", "Clurow"),
            Pair.of("Haslett", "Grishanov"),
            Pair.of("Wilmer", "Rapier"),
            Pair.of("Whittaker", "Carmel"),
            Pair.of("Tomasine", "Breeder"),
            Pair.of("Margie", "Ginsie"),
            Pair.of("Martha", "Bodechon"),
            Pair.of("Allison", "Kolakowski"),
            Pair.of("Olenka", "Rannie"),
            Pair.of("Carly", "Risely"),
            Pair.of("Rosalie", "Williscroft"),
            Pair.of("Sherye", "Eddicott"),
            Pair.of("Thorstein", "Sollas"),
            Pair.of("Gregor", "Abrahamsohn"),
            Pair.of("Babbette", "Ginger"),
            Pair.of("Gil", "Galletly"),
            Pair.of("Pippy", "Chittenden"),
            Pair.of("Hanan", "Tewkesbury"),
            Pair.of("Laurette", "Antognozzii"),
            Pair.of("Vic", "Byart"),
            Pair.of("Sibyl", "Daelman"),
            Pair.of("Flin", "Dwerryhouse"),
            Pair.of("Silvia", "Gecke"),
            Pair.of("Harlan", "Turbern"),
            Pair.of("Bambie", "Pitts"),
            Pair.of("Fenelia", "Duerden"),
            Pair.of("Dallis", "Dyzart"),
            Pair.of("Galvin", "Manon"),
            Pair.of("Roobbie", "Highnam"),
            Pair.of("Dev", "Lanyon"),
            Pair.of("Hank", "Clementet"),
            Pair.of("Isabel", "Noell"),
            Pair.of("Sibilla", "Annett"),
            Pair.of("Salim", "Burril"),
            Pair.of("Erick", "Humbles"),
            Pair.of("Yankee", "Walby"),
            Pair.of("Fransisco", "Haddacks"),
            Pair.of("Lin", "Wines"),
            Pair.of("Hall", "Rubenczyk"),
            Pair.of("Cosmo", "Tantum"),
            Pair.of("Loren", "Trowell"),
            Pair.of("Powell", "Toleman"),
            Pair.of("Rollins", "Satterthwaite"),
            Pair.of("Costanza", "Raggles"),
            Pair.of("Latrina", "Sproule"),
            Pair.of("Julee", "Cratchley"),
            Pair.of("Erin", "Hartzog"),
            Pair.of("Darsey", "Woollard"),
            Pair.of("Gennie", "Poyntz"),
            Pair.of("Jeri", "Nester"),
            Pair.of("Skipp", "Elkin"),
            Pair.of("Jessika", "Sabathier"),
            Pair.of("Cindra", "Burrass"),
            Pair.of("Jolene", "Ascraft"),
            Pair.of("Kial", "Enefer"),
            Pair.of("Frayda", "Seegar"),
            Pair.of("Lucien", "Ecles"),
            Pair.of("Wally", "Burel"),
            Pair.of("Cornelius", "Silk"),
            Pair.of("Mahmud", "Elcoux"),
            Pair.of("Vaughan", "Somerlie"),
            Pair.of("Jeth", "Spera"),
            Pair.of("Eadie", "Tithecote"),
            Pair.of("Hetty", "Scullin"),
            Pair.of("Dniren", "Crush"),
            Pair.of("Lemmy", "Kean"),
            Pair.of("Jo-anne", "Blevin"),
            Pair.of("Mel", "Springtorpe"),
            Pair.of("Chickie", "Pounsett"),
            Pair.of("Jehu", "Cuell"),
            Pair.of("Ulberto", "Goor"),
            Pair.of("Terri", "McDunlevy"),
            Pair.of("Suzanna", "Spering"),
            Pair.of("Tris", "Mart"),
            Pair.of("Prisca", "Windle"),
            Pair.of("Daniel", "Stribbling"),
            Pair.of("Tildi", "Filyakov"),
            Pair.of("Sawyere", "Bing"),
            Pair.of("Delbert", "Hesser"),
            Pair.of("Tedra", "Cutchee"),
            Pair.of("Jessa", "Pane"),
            Pair.of("Bridget", "Moggach"),
            Pair.of("Joline", "Calbaithe"),
            Pair.of("Orv", "Carruthers"),
            Pair.of("Sydel", "Chastey"),
            Pair.of("Griffie", "Basterfield"),
            Pair.of("Gwendolen", "Davenall"),
            Pair.of("Suki", "Brunelleschi"),
            Pair.of("Robinson", "Marks"),
            Pair.of("Pepillo", "Pardy"),
            Pair.of("Amandi", "Mitchenson"),
            Pair.of("Ronnica", "Jefferd"),
            Pair.of("Jeniece", "Moncey"),
            Pair.of("Ronny", "Pawlata"),
            Pair.of("Hazel", "Fittis"),
            Pair.of("Immanuel", "Allward")};
}
