package net.redborder.samza.tasks;

import Exceptions.ConfigurationException;
import Exceptions.RBInstance;
import moa.cluster.Cluster;
import moa.cluster.Clustering;
import moa.cluster.SphereCluster;
import moa.clusterers.outliers.MCOD.MCOD;
import moa.gui.visualization.DataPoint;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnomalyDetectionTask implements StreamTask, InitableTask /*, WindowableTask */{

    /**
     * Con este objeto podremos hacer loggin y seguir las acciones del programa
     */
    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionTask.class);

    /**
     * Con este objeto podremos asociar los flujos que recibe el detector con el conjunto de datos que utilizara
     * cada vez que se llegue al limite pasaremos a comprobarlo.
     */
    private Map<String, LinkedList<Instances>> instanciasFlujo = new HashMap<String, LinkedList<Instances>>();

    /**
     * Con este objeto podrmeos asociar a cada flujo de entrada su salida correspondiente
     */
    private Map<String, SystemStream> salidaSistema = new HashMap<String, SystemStream>();

    /**
     * Con este objeto podremos mapear los detectores a los que pertenece cada flujo, de manera que podremos enviar a cada
     * uno las instancias correspondientes a cada flujo con el objeto "instanciasFlujo"
     */
    private Map<String, Map<String, MCOD>> detectorFlujo = new HashMap<String, Map<String,MCOD>>();

    /**
     * Con este objeto podremos manipular la instancia actual del mensaje del flujo recibido, identificando previamente
     * este flujo.
     */
    private Instances datasetActual = null;

    /**
     * Objeto que permite obtener el mapeo de los JSON
     */
    private ObjectMapper _mapper = new ObjectMapper();

    /**
     * Cadena que nos permitira identificar el flujo del que proviene el mensaje recibido
     * se va actualizando por cada mensaje que se recibe.
     */
    private String flujoRecibido = "<Flujo sin especificar>";
    /**
     * Con esta variable tendremos una marca temporal de la instancia recibida
     */
    private int timeStamp = 0;

    /**
     * Con esta variable tendremos el total de instancias procesadas
     */
    private int procesadas = 0;

    /**
     * Con este objeto tendremos disponible un buffer para el almacenamiento de los DataPoints
     */
    private LinkedList<DataPoint> buffer = new LinkedList<DataPoint>();


    // ================================================================================================= //
    //                                           CONSTANTES                                              //
    // Con las constantes podrmeos definir los distintos campos de la configuracion de manera que sera   //
    // facil de editar en el caso que se quiera corregir un error o similar.                             //
    // ================================================================================================= //

    // Parametro para especificar el flujo de entrada
    private final String PARAM_INPUT_STREAM = "input";
    // Parametro para espcificar las  clases a monitorizar
    private final String PARAM_CLASSES = "classes";
    // Parametro para especificar el tipo de detector de anomalias
    private final String PARAM_OUT_DET = "outlier_detection";
    // Parametro para especificar el tipo de accion en la configuracion
    private final String PARAM_ACTION = "action";
    // Parametros para especificar el flujo de salida
    private final String PARAM_OUTPUT_STREAM = "output";

    /**
     * Metodo que permite aplicar una configuracion a la actual tarea de Samza
     * @param configuracion Asociaciones por parejas clave:valor de cada uno de los parametros
     * @throws ConfigurationException En el caso de incumplir con algun criterio
     */
    /*
    public void aplicarConfiguracion(Map<String, Object> configuracion) throws ConfigurationException {

        // ================================================================================================= //
        //                                           INPUT                                                   //
        // Se encargara de identificar el flujo de entrada que se quiere monitorizar, es necesario este paso //
        // si se desea que el detector inspeccione los atributos.                                            //
        // ================================================================================================= //

        log.info("Configuracion que se aplicara : " + configuracion.toString());

        // Comprobamos si se ha especificado el flujo de entrada, en caso de no haberlo hecho se lanza una excepcion
        if(!configuracion.containsKey(PARAM_INPUT_STREAM))
            throw  new ConfigurationException("Es obligatorio el uso del parametro \"" + PARAM_INPUT_STREAM + "\" para definir el flujo objetivo de entrada");

        // Primero obtenemos el nombre del flujo que se va a tratar
        String flujoEntrada = configuracion.get(PARAM_INPUT_STREAM).toString();

        log.info("El flujo de entrada sera : \"" + flujoEntrada + "\"");

        // ================================================================================================= //
        //                                           ACTION                                                  //
        // Basicamente indicara si lo que se desea es crear o eliminar la configuracion, observese que no    //
        // existe la posibilidad de modificacion, dado que al modificar una configuracion es necesario       //
        // destruir el detector, por ello siempre se puede crear y sustituir la configuracion obteniendo el  //
        // mismo resultado. Al eliminar la configuracion la eliminamos en su totalidad                          //
        // ================================================================================================= //

        // Comprobamos si se ha especificado la accion de la configuracion, en caso contrario lanzamos una excepcion
        if(!configuracion.containsKey(PARAM_ACTION))
            throw new ConfigurationException("Es obligatorio el uso del parametro \"" + PARAM_ACTION + "\" para definir el tipo de configuracion");

        // Por ultimo obtenemos la accion de la aconfiguracion
        String accion = configuracion.get("action").toString();

        // Comprobamos si la configuracion contiene los parametros adecuados
        if(!accion.matches("(create|delete)"))
            throw new ConfigurationException("El parametro \"" + accion + "\" no es valido como opcion para el atributo \"action\"\n" + configuracion.toString());

        // Si estamos creando una nueva configuracion
        if(accion.equals("create")){

            // Aplicamos unos valores por defectos
            int windows = 350000;     // Ventana para 1000 instancias
            double radius = 0.1;    // Radio de 0.1
            int neightbours = 5;   // Numero de vecinos de 50

            // ================================================================================================= //
            //                                           CLASSES                                                 //
            // Se encargara de identificar las clases en las que se agrupan los atributos, una instancia puede   //
            // tener de 1 a N clases. Por ejemplo, una instancia puede tener una clase "IP_X" si el atributo es  //
            // una IP con X siendo origen o destino en el caso de eventos de seguridad o puede tener una clase   //
            // "persona" siendo persona la union de los atributos peso, altura y edad en el caso clinico.        //
            // ================================================================================================= //

            // Comprobamos si se ha especificado las clases que se van a monitorizar, en caso de no haberlo hecho se lanza una excepcion
            if(!configuracion.containsKey(PARAM_CLASSES))
                throw new ConfigurationException("Es obligatorio el uso del parametro \"" + PARAM_CLASSES + "\" para definir los tipos de instancias a monitorizar");

            // Segundo obtenemos las clases que sean para crear
            Map<String, Object> clases = (LinkedHashMap<String, Object>)configuracion.get(PARAM_CLASSES);

            log.info("Obtenida las siguientes clases con sus respectivos atributos y tipos : " + clases.toString());

            // ================================================================================================= //
            //                                           OD_TYPE                                                 //
            // Tipo de detector, aun no esta aprobado, la idea es indicar si es un detector de instancia unica   //
            // esto se da cuando los atributos comparten una distribucion estadistica similar y muy aproximada o //
            // si tendra por el contrario un detector por cada tipo de atributo de manera que cada detector      //
            // inspecciona un atributo.                                                                          //
            // ================================================================================================= //

            // Por defecto el detector sera distinto para cada clase
            String ODType = "dedicated";

            // Comprobamos si se ha especificado el tipo de detectores a utilizar
            if(configuracion.containsKey(PARAM_OUT_DET))
                // Tercero obtenemos el tipo de detector que se va a utilizar
            {
                // Obtenemos la configuracion concreta del detector
                Map<String, Object> configDetector = (Map<String, Object>) configuracion.get(PARAM_OUT_DET);

                // Comprobamos si contiene el tipo del detector
                if(configDetector.containsKey("type"))
                    ODType = configDetector.get("type").toString();

                // Comprobamos si contiene los parametros del detector
                if(configDetector.containsKey("parameters")) {
                    // Obtenemos los parametros
                    Map<String, Object> parameters = (Map<String, Object>) configDetector.get("parameters");
                    // Asignamos el radio
                    radius = Double.valueOf(parameters.get("R").toString());
                    // Asignamos la ventana
                    windows = Integer.valueOf(parameters.get("W").toString());
                    // Asignamos el numero de vecinos
                    neightbours = Integer.valueOf(parameters.get("K").toString());
                }
            }


            log.info("Se va a proceder a crear una nueva configuracion para el flujo \"" + flujoEntrada + "\"");
            // Creamos un nuevo array de detectores al que se le asociara la clase que monitorizara, a un detector se le

            // asignara una clase en lugar de los atributos, ya que una clase puede estar compuesta por varios atributos
            Map<String, MCOD> arrayDetectores = new HashMap<String, MCOD>();

            // Creamos una lista enlzada de los datasets que tendra el detector, por cada detector sera necesario un dataset
            LinkedList<Instances> datasets = new LinkedList<Instances>();

            // Si el tipo de motor que necesitamos es unico para cada clase entonces creamos un array de motores
            if(ODType.matches("dedicated")){

                log.info("Aplicaremos la configuracion para varios detectores");

                // Para cada clase tendremos un detector en concreto
                log.info("Creacion del conjunto de atributos para instancias de MOA de las clases seleccionadas: " + clases.keySet());

                // Con esta variable numeraremos los datasets
                int numeroDataset = 0;

                // Obtenemos todas las clases que hay definidas
                for(String s : clases.keySet()){

                    // -----------------------------------------------------------

                    // Creamos un array de atributos obteniendo para ello los dados por el JSON
                    ArrayList <Attribute> atributos = new ArrayList<Attribute>();

                    // Creamos un array con las clases que el detector va a tratar, definidas en el JSON
                    ArrayList<String> clasesDefinidas = new ArrayList<String>();

                    // El numero de atributos sera al inicio de 1
                    int maxNumeroAtributos = 1;

                    log.info("Se va a proceder a crear los atributos para la clase : \"" + s + "\"");

                    // Obtenemos los atributos para la clase que se ha seleccionado
                    Map<String, Object> atributosYTipos = (LinkedHashMap<String, Object>)clases.get(s);

                    // Comprobamos si el numero de atributos es mayor al que se inicializo anteriormente de ser asi se aplica
                    // dicho valor, de no ser asi se deja el valor que tenia por defecto
                    maxNumeroAtributos = atributosYTipos.size() > maxNumeroAtributos ? atributosYTipos.size() : maxNumeroAtributos;

                    // Obtenemos los atributos y los tipos
                    for(Object a : atributosYTipos.keySet()){

                        log.info("Obtenido el atributo : " + a.toString());
                        log.info("Obtenido el atributo : " + a.toString());
                        *//**
                         * Parte opcional que aun no esta confirmada
                         *//*
                        if(!a.toString().equals("option")) {
                            if (atributosYTipos.get(a.toString()).toString().matches("N")) {
                                // Lo tratamos como un numero ENTERO
                                atributos.add(new Attribute(a.toString()));
                            } else if (atributosYTipos.get(a.toString()).toString().matches("R")) {
                                // Lo tratamos como un numero REAL
                                atributos.add(new Attribute(a.toString()));
                            } else {  // Lo tratamos como cadena
                                atributos.add(new Attribute(a.toString(), (ArrayList) null));
                            }
                        }else{
                            if(atributosYTipos.get(a.toString()).toString().equals("equals")){
                                log.info("Se va a proceder a aplicar los mismos atributos para la clase : " + s);
                            }
                        }

                    } // for de atributos

                    log.info("La clase \"" + s + "\" estara compuesta por " + maxNumeroAtributos + " atributos");

                    // Agreagamos la nueva clase

                    clasesDefinidas.add(s);
                    // agregamos una nueva serie para monitorizar dicha clase

                    // Procuraremos siempre agregar la clase como el ultimo elemento, asi a la hora de crear la instancia
                    // podremos indicar que el indice en el que se encuentra las clases sera la longitud del vector - 1
                    // esto es una ventaja a la hora de aumentar o disminuir el dataset.
                    atributos.add(new Attribute("class", clasesDefinidas));

                    log.info("Se ha creado el conjunto de clases : " + clasesDefinidas.toString());

                    // El conjunto de instancias tendra como nombre el mismo que el del flujo, para poder identificarlo de
                    // una forma sencilla, contendra los atributos definidos en el JSON y por defecto tendran 1000 instancias
                    // de capacidad
                    Instances dataset = new Instances(flujoEntrada + "_dataset_" + numeroDataset++, atributos, 1000);
                    // Con el valor -1 indicamos que las clases para la clasificacion es el ultimo atributo agregado
                    dataset.setClassIndex(atributos.size() - 1);

                    log.info("Se ha creado un nuevo dataset con las siguientes caracteristicas : \n\n" + dataset.toSummaryString());

                    datasets.add(dataset);
                    // -----------------------------------------------------------

                    // Creamos un nuevo detector con los valores establecidos (por defecto si no es asi)
                    MCOD detector = new MCOD();
                    detector.kOption.setValue(neightbours);
                    detector.windowSizeOption.setValue(windows);
                    detector.radiusOption.setValue(radius);
                    // Lo preparamos para su uso con los parametros establecidos (por defecto si no es asi)
                    detector.prepareForUse();

                    // Asociamos al detector la clase especifica para el, esta clase contendra los atributos necesarios
                    arrayDetectores.put(dataset.relationName(), detector);
                    log.info("Se ha creado y agregado un nuevo detector para la clase : \"" + s + "\"");

                }

                // Asociamos el conjunto de datasets al flujo a monitorizar
                instanciasFlujo.put(flujoEntrada, datasets);

                // Asociamos el array de detectores al flujo a monitorizar
                detectorFlujo.put(flujoEntrada, arrayDetectores);
                log.info("Se ha agregado un nuevo array de detectores asociado al flujo \"" + flujoEntrada + "\" de " + arrayDetectores.size() + " detector(es)");
                log.info("Se ha creado un conjunto de datasets de " + datasets.size() + " dataset(s)");

            }


            // ================================================================================================ //
            //                                           OUTPUT                                                 //
            // ================================================================================================ //

            // Obtenemos la salida de los resultados
            if(configuracion.containsKey(PARAM_OUTPUT_STREAM)){
                Map<String, String> salida = (Map<String,String>) configuracion.get(PARAM_OUTPUT_STREAM);

                // Obtenemos el sistema
                String sistema = salida.get("system");
                // Obtenemos su flujo de salida
                String flujoSalida = salida.get("stream");

                // Asociamos al flujo de entrada su correspondiente salida
                salidaSistema.put(flujoEntrada, new SystemStream(sistema, flujoSalida));

                log.info("Se ha agregado la salida del flujo al flujo \"" + flujoSalida + "\" del sistema \"" + sistema + "\" ");
            }

        }else if(accion.equals("delete")){

            log.info("Se va a proceder a eliminar una configuracion existente para el flujo \"" + flujoEntrada + "\"");
            // Eliminamos la instancia de flujo asociada
            instanciasFlujo.remove(flujoEntrada);
            // Eliminamos el detector del flujo asociado
            detectorFlujo.remove(flujoEntrada);

            salidaSistema.remove(flujoEntrada);

            log.info("Se ha eliminado el dataset, el flujo de salida y los detectores relacionados de forma satisfactoria");

        }
    }
*/
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

        // Obtenemos el nombre del flujo
        flujoRecibido = envelope.getSystemStreamPartition().getStream();

        log.info("Se ha recibido un nuevo mensaje del flujo : " + flujoRecibido);

        // Comprobamos si el mensaje que hemos recibido es de configuracion, de ser asi
        if(instanciasFlujo.containsKey(flujoRecibido)) {  // En caso contrario aplicamos el algoritmo al evento que se obtenga si se ha creado previamente

            log.info("Se va a proceder a tratar los datos recibidos para el flujo \"" + flujoRecibido + "\"");

            String mensaje = envelope.getMessage().toString();

            log.info("RAW del mensaje : " + mensaje);

            // Con este objeto podremos obtener el JSON parseado y manipularlo de forma mas comoda
            // Creamos un mapeo de clave/valor para el evento que obtenemos del mensaje
            // Basicamente lo que estamos haciendo es coger nuestro JSON y hacerle un mapeado para que de esa manera
            // tengamos una acceso mas simple a los valores.
            Map<String, Object> evento = (Map<String, Object>)envelope.getMessage();

            log.info("Recibido en el mensaje los valores : " + evento.toString());

            // Obtenemos el conjunto de datasets asociados al flujo
            LinkedList<Instances> datasetsObtenidos = instanciasFlujo.get(flujoRecibido);

            Map<String, MCOD> detectoresAsociados = detectorFlujo.get(flujoRecibido);

            // Para cada dataset
            for(int i = 0; i < datasetsObtenidos.size(); i++){
                // Obtenemos el actual
                datasetActual = datasetsObtenidos.get(i);

                // Creamos la instancia
                RBInstance instancia = new RBInstance(datasetActual.numAttributes());
                // Le asignamos el dataset
                instancia.setDataset(datasetActual);
                // Escribimos el JSON que hemos obtenido como evento
                instancia.setEvent(_mapper.writeValueAsString(evento));

                log.info("Se ha creado una nueva instancia para el flujo " + flujoRecibido + " con " + instancia.numAttributes() + " atributos");

                log.info("Se ha obtenido la clase : " + instancia.classAttribute().value(0));

                if(instancia.classAttribute().value(0).equals("src")){

                    int [] octetos = ip2int(evento.get("src").toString());

                    for(int j = 0; j < instancia.numAttributes(); j++){

                        if(j < 4)
                            instancia.setValue(j, octetos[j]);
                        else
                            instancia.setValue(j, "src");
                    }

                }else{

                    // De la instancia creada
                    for (int j = 0; j < instancia.numAttributes(); j++) {
                        // Obtenemos el atributo
                        String atributo = instancia.attribute(j).name();
                        log.info("Se ha obtenido el atributo : " + atributo);

                        // Comprobamos el tipo de atributo, en el caso que sea numerico
                        if (instancia.attribute(j).isNumeric()) {
                            instancia.setValue(j, Double.valueOf(evento.get(atributo).toString()));
                            // En el caso de que el atributo sea una cadena
                        } else if (instancia.attribute(j).isString()) {
                            instancia.setValue(j, evento.get(atributo).toString());
                            // En caso de que no fuera nada de lo anterior
                        } else {

                            // Obtenemos el numero de atributos
                            Enumeration<Object> atributos = instancia.classAttribute().enumerateValues();

                            // Mientras que haya atributos
                            while(atributos.hasMoreElements()) {
                                // Obtenemos la clase nominal
                                String claseNominal = atributos.nextElement().toString();
                                log.info("Valor de clase posible : " + claseNominal);

                                // Si la clase nominal coincide con el atributo entonces
                                if(claseNominal.equals(atributo))
                                    // Obtenemos el valor del atributo del evento y lo asignamos a la instancia
                                    instancia.setValue(j, evento.get(atributo).toString());
                                else {  // Si no coincide
                                    log.info("No se ha encontrado una clave para el atributo : " + atributo);
                                    // agregamos a la instancia el valor de la clase nominal
                                    instancia.setValue(j, claseNominal);
                                }
                            }

                        }
                    }

                }

                log.info("Se ha creado la instancia con valores : " + instancia.toString());

                // agregamos al dataset la nueva instancia
                datasetActual.add(instancia);
                log.info("Se ha agregado la instancia al dataset \"" + datasetActual.relationName() + "\" con exito");

                log.info("El dataset actual \"" + datasetActual.relationName() + "\" tiene un tamanio de " + datasetActual.size());

                // Obtenemos el detector basandonos en el nombre de la relacion del dataset
                MCOD detector = detectoresAsociados.get(datasetActual.relationName());

                log.info("DATASET OBTENIDO PARA PROCESADO : " + datasetActual.toSummaryString());

                // Entrenamos al detector con la instancia
                detector.trainOnInstanceImpl(instancia);

                // Una vez alcanzado el limite del dataset
                if(datasetActual.size() % 350000 == 0) {
//                    log.info("Se han llegado a las 1000 instancias");

                    // Obtenemos todos los outliers detectados
/*
                    for (int k = 0; k < detector.getOutliersResult().size(); k++) {
                        // Obtenemos la instancia anomala
                        RBInstance instanciaAnomala = (RBInstance)detector.getOutliersResult().get(k).inst;
                        log.info("Instancia detectada como anomala : " + instanciaAnomala.getEvent());
                        // Creamos el evento asociado a la instancia
                        Map<String, Object> eventoAnomalo = (Map<String, Object>)_mapper.readValue(instanciaAnomala.getEvent(), Map.class);
                        // agregamos la nueva cadena donde ponemos el nuevo valor "outlier"
                        eventoAnomalo.put("outlier","[" + instancia.classAttribute().name() + "]");

                        log.info("Evento ANOMALO : " + _mapper.writeValueAsString(eventoAnomalo));
                        // Reenviamos por el sistema que se ha escogido
//                        collector.send(new OutgoingMessageEnvelope(salidaSistema.get(flujoRecibido), _mapper.writeValueAsString(eventoAnomalo)));

                    }
*/

/*
                    for(int l = 0; l < detector.getClusteringResult().size(); l++){
                        SphereCluster sc = (SphereCluster) detector.getClusteringResult().get(l);
                        log.info("Informacion del cluster : " + sc.getInfo() );
                    }
*/

                    // Limpiamos el contenido del dataset actual
                    datasetActual.clear();
                }

                // Obtenemos las estadisticas
                log.info(detector.getStatistics());

            }

        }else{
            log.info("El flujo \"" + flujoRecibido + "\" no sera tratado por no estar especificado en la configuracion");
        }

    }

    public int[] ip2int(String ipAddress){

        int[] octetos = {0, 0, 0, 0};

        Matcher m = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}").matcher(ipAddress);

        if(m.matches()){
            String[] octets = ipAddress.split("\\.");

            int i = 3;

            for(String octet : octets){

                octetos[i] = Integer.parseInt(octet);
                i--;
            }

        }else{
            log.error("ERROR! To parse IP");
        }

        return octetos;

    }


    @SuppressWarnings("unchecked")
    @Override
    public void init(Config conf, TaskContext context) throws Exception {
        // Permitimos los campos que no estan delimitado por comillas dobles
        _mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        // Permitimos los campos que estan delimitados por comillas simples
        _mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        // Obtenemos el listado de flujos de entrada
        List<String> inputStreams = conf.getList("outlier.input");

        // Para cada flujo del listado
        // Comprobamos si tiene definida las clases
        for(String stream : inputStreams)

            if (conf.containsKey("outlier." + stream + ".classes")) {

                int numDataset = 0;

                // Obtenemos el listado de clases para el flujo definido
                List<String> classes = conf.getList("outlier." + stream + ".classes");
                // Lista enlazada de datasets
                LinkedList<Instances> datasets = new LinkedList<Instances>();
                // Mapeado de detectores
                Map<String, MCOD> arrayDetectores = new HashMap<String, MCOD>();

                // Tomaremos las propias clases como atributos de cadena, de forma que
                // cada clase es en sí un atributo de cadena
                for (String cls : classes) {

                    // Listado de atributos
                    ArrayList<Attribute> atributos = new ArrayList<Attribute>();

                    if(conf.containsKey("outlier." + stream + ".class." + cls)){

                        List<String> attDefined = conf.getList("outlier." + stream + ".class." + cls);

                        for(String att : attDefined){

                            Matcher m = Pattern.compile("(?<name>[_\\w][\\w\\d]*):(?<type>string|numeric|IP)").matcher(att);

                            if(m.matches()){

                                String name_att = m.group("name");

                                if(m.group("type").equals("string")){
                                    atributos.add(new Attribute(name_att, (ArrayList) null));
                                }else if(m.group("type").equals("numeric")){
                                    atributos.add(new Attribute(name_att));
                                }else if(m.group("type").equals("IP")){
                                    for(int i = 1; i <= 4 ; i++)
                                        atributos.add(new Attribute("IP_segment_"+i));
                                }

                                log.info("Added attribute : " + name_att);
                            }

                        }

                    }else{
                        // Añadimos un nuevo atributo con el nombre del campo del flujo
                        atributos.add(new Attribute(cls,(ArrayList) null));
                        log.info("Added attribute : " + cls);
                    }


                    ArrayList<String> clasesNominales = new ArrayList<String>();
                    clasesNominales.add(cls);
                    // Añadimos un nuevo atributo de clases con el nombre del campo del flujo
                    atributos.add(new Attribute("class", clasesNominales));
                    log.info("Added attribute class with content : " + clasesNominales.toString());

                    // Creamos un nuevo dataset
                    Instances dataset = new Instances(stream + "_dataset_" + numDataset++, atributos, 1000);
                    dataset.setClassIndex(atributos.size() - 1);

                    log.info("Created dataset : \n\n" + dataset.toSummaryString());

                    datasets.add(dataset);

                    MCOD detector = new MCOD();
                    log.info("Created new outlier detector");
                    if (conf.containsKey("outlier." + stream + "." + cls + ".parameters")) {
                        log.info("Setting outlier detector parameters");
                        List<String> parametros = conf.getList("outlier." + stream + "." + cls + ".parameters");

                        for(String option : parametros){
                            String[] opt = option.split(":");

                            if(opt[0].matches("window")){
                                detector.windowSizeOption.setValue(Integer.valueOf(opt[1]));
                            }else if(opt[0].matches("radius")){
                                detector.radiusOption.setValue(Double.valueOf(opt[1]));
                            }else if(opt[0].matches("k")){
                                detector.kOption.setValue(Integer.valueOf(opt[1]));
                            }else{
                                throw new ConfigurationException("Not valid! : " + opt[0]);
                            }
                        }
                    }
                    detector.prepareForUse();

                    log.info("Outlier Detector prepared with parameters: \n "
                            + "\twindow: " + detector.windowSizeOption.getValue() + "\n"
                            + "\tradius: " + detector.radiusOption.getValue() + "\n"
                            + "\tk: " + detector.kOption.getValue() + "");

                    log.info("Adding outlier detector with dataset : " + dataset.relationName());
                    arrayDetectores.put(dataset.relationName(), detector);

                }

                log.info("Adding datasets with stream : " + stream);
                instanciasFlujo.put(stream, datasets);
                log.info("Adding array of outlier detector with stream : " + stream);
                detectorFlujo.put(stream, arrayDetectores);

                log.info("Finish configuration of MOA");
            } else {
                // En caso contrario lanzamos una excepción de configuración
                throw new ConfigurationException("Don't defined outlier." + stream + ".classes");
            }

    }

    /**
     * Metodo que permite monitorizar las instancias de forma individual
     * @param instancia Instancia a inspeccionar de forma individual
     */
    public void inspeccionarConDetectorIndividualmente(Instance instancia) {

        // Obtenemos el array de detectores
        Map<String, MCOD> arrayDetectores = detectorFlujo.get(flujoRecibido);

        // COmprobamos si tenemos uno o mas detectores
        if (arrayDetectores.size() >= 1) {

            // Obtenemos el detector actual para dicha clase
            MCOD detectorActual = arrayDetectores.get(instancia.dataset());
            // Entrenamos el detector
            detectorActual.trainOnInstanceImpl(instancia);
            log.info(detectorActual.getStatistics());

            // Obtenemos el actual radio del detector
            double radioActualDetector = detectorActual.radiusOption.getValue();
            // Obtenemos la densidad actual del detector
            double densidadActualDetector = detectorActual.kOption.getValue();

            // Convertimos nuestra instancia
            RBInstance dInstancia = (RBInstance) instancia;

            // Asignamos un espacio temporal
            timeStamp++;
            // Obtenemos el numero de muestras procesadas
            procesadas++;

            // Creamos un nuevo punto para esa instancia en un tiempo fijo de tiempo
            DataPoint point = new DataPoint(instancia, timeStamp);
            // Almacenamos el punto en el buffer
            buffer.add(point);

            // Si superamos el limite de ventana
            if (buffer.size() > 1000) {
                // entonces eliminamos el primer elemento (que sera el mas antiguo)
                buffer.removeFirst();
            }

            // Creamos un nuevo cluster
            Clustering gtCluster = null;

            // Para cada punto de dato del buffer
            for (DataPoint dp : buffer) {
                // Actualizamos si peso basandonos en el espacio de tiempo asignado
                dp.updateWeight(timeStamp, 1000);
                // Y creamos un nuevo cluster con dicha informacion
                gtCluster = new Clustering(new ArrayList<DataPoint>(buffer));
            }

            log.info("Tamanio del cluster : " + gtCluster.size() + " cluster/s");

            // Variable para el procesado de los radios
            double radioAnterior = 0;

            // Para cada cluster que contiene
            for (Cluster c : gtCluster.getClustering()) {

                SphereCluster sc = (SphereCluster) c;

                log.info("[CLASE]RADIO GT : " + sc.getRadius());

                if (procesadas % 100 == 0)
                    if (radioAnterior == 0) {
                        radioAnterior = sc.getRadius();
//                    } else if (sc.getRadius() - radioAnterior <= EPSILON) {
//                        sumaRadios += sc.getRadius();
                    } else {
                        log.info("Se ha detectado un radio elevado, no se agregara a las estadisticas");
                    }

                log.info("[CLASE]DENSIDAD : " + sc.getWeight());

            }

//            }

        } else {
            log.info("No existen referencia a detectores para el flujo \"" + flujoRecibido + "\"");
        }
    }

    /**
     * este metodo permite normalizar el conjunto de datos, de esta forma podemos aplicar escalado con
     * valores comprendidos entre 0 y 1. Esto es necesario si la dimension de los valores son muy desproporcionadas.
     * @param dataset Conjunto de instancias que se van a normalizar
     */
    public void normalizarDataset(Instances dataset){

        // Creamos un minimo con el valor mas alto de un Double
        double min = Double.POSITIVE_INFINITY;
        // Creamos el maximo con el valor mas pequeño de un Double
        double max = Double.NEGATIVE_INFINITY;
        // Diferencia entre el maximo y el minimo sera 0.0
        double diffMaxMin = 0.0;

        for (int i = 0 ; i < dataset.numInstances(); i++){
            // Para cada instancia del dataset
            Instance instancia = dataset.get(i);
            for (int j = 0; j < instancia.numAttributes(); j++) {

                // Obtenemos el valor de sus atributos
                double value = instancia.value(j);
                // Obtenemos el minimo valor de todo el dataset
                if(value < min)
                    min = value;
                // Obtenemos el maximo valor de todo el dataset
                if(value > max)
                    max = value;
            }
        }

        // Una vez obtenido el maximo y el minimo efectuamos la diferencia de ambos
        diffMaxMin = max - min;

        for (int i = 0; i < dataset.numInstances(); i++){
            // Para cada instancia del dataset
            Instance instancia = dataset.get(i);

            for (int j = 0; j < instancia.numAttributes() ; j++) {
                // Comprobamos que la diferencia sea distinto de 1 y de 0
                if(diffMaxMin != 1 && diffMaxMin != 0){
                    // Obtenemos el valor de la instancia que hemos obtenido
                    double v = instancia.value(j);
                    // Hacemos la diferencia con el minimo valor que se ha obtenido y lo dividimos entre
                    // la diferencia del maximo y el minimo
                    v = (v - min)/diffMaxMin;
                    // Establecemos el nuevo valor para la instancia que hemos procesado
                    instancia.setValue(j, v);
                }
            }// For de atributos de instancia

        }// For de instancias

    }

/*
    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator)
            throws Exception {
    }
*/
}
