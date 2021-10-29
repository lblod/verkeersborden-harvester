require 'linkeddata'
require 'csv'
require 'digest'
require 'securerandom'

class InstructieHarvester
  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DC = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  QB = RDF::Vocabulary.new("http://purl.org/linked-data/cube#")
  REGORG = RDF::Vocabulary.new("https://www.w3.org/ns/regorg#")
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  NFO = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")
  NIE =  RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/01/19/nie#")
  DBPEDIA = RDF::Vocabulary.new("http://dbpedia.org/ontology/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  SV = RDF::Vocabulary.new('http://www.w3.org/2003/06/sw-vocab-status/ns#')

  MOB = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/mobiliteit#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
  LBLOD_MOW = RDF::Vocabulary.new("http://data.lblod.info/vocabularies/mobiliteit/")

  def initialize(input_instructie, input_cache)
    @csv_path = input_instructie
    @sign_instructions = {}
    begin
      @sign_cache = {}
      ::CSV.foreach(input_cache, {  headers: :first_row, encoding: 'utf-8', quote_char: "\""  }) do |row|
        if row["input_code"] && row["our_code"] && row["our_code"].length != 0
          @sign_cache["input_code"] = { }
        end
        @sign_cache[row["input_code"]] = { sign: RDF::URI.new(row["uri"]), type: RDF::URI.new(row["type"]), label: row["our_code"]}
      end
    rescue => e
      puts e
      @sign_cache = {}
    end
    @output = "output/verkeersborden-combinaties.ttl"
  end


  def harvest
    index = 1
    begin
      # group rows that belong together first
      maatregel_input = {}
      ::CSV.foreach(@csv_path, {  headers: :first_row, encoding: 'utf-8', quote_char: "\""  }) do |row|
        uuid = row.first[1]
        if maatregel_input.has_key?(uuid)
          maatregel_input[uuid] << row
        else
          maatregel_input[uuid] = [row]
        end
      end

      maatregel_input.each do |key, rows|
        first_row = rows[0]
        name = first_row["maatregel_naam"]
        if name.include?("/") || name.include?("-") || name.include?("+")
          measure_signs = parse_name(name)
        else
          measure_signs = [map_sign_code_to_uri(name)]
        end

        all_signs_mapped = measure_signs.detect(Proc.new{"ALL_MAPPED"}){ |sign| sign.nil? || ! sign[:sign]} === "ALL_MAPPED" # returns true if nothing matches our condition
        if all_signs_mapped
          build_data_for_measure(key,rows, measure_signs)
          build_data_for_signs(rows, measure_signs)
        else
          puts "ignoring measure with id #{key} because not all signs could be matched"
        end
      end

      @sign_instructions.each do |sign, value|
        RDF::Graph.new do |graph|
          value[:instructions].each do |instruction|
            template_uuid = SecureRandom.uuid
            template_uri = RDF::URI.new "http://data.lblod.info/templates/#{template_uuid}"
            graph.insert([sign, EXT.template, template_uri])
            graph.insert([template_uri, RDF.type, EXT.Template])
            graph.insert([template_uri, MU.uuid, template_uuid])
            graph.insert([template_uri, EXT.value, "<p>#{instruction}</p>"])
          end
          File.write("./output/instructions-for-#{value[:label]}.ttl", graph.dump(:ttl), mode: 'w')
        end
      end
      CSV.open("./output/sign-mapping.csv", "wb") do |csv|
        csv << ["input_code", "our_code", "uri", "type"]
        @sign_cache.each do |key, value|
          csv << [key, value[:label], value[:sign], value[:type]]
        end
      end
    rescue StandardError => e
      puts e.trace
    end
  end

  def build_data_for_signs(rows, measure_signs)
    instructions = get_instructions(rows)
    if measure_signs.length == instructions.length
      measure_signs.each_with_index do |sign, index|
        sign_uri = sign[:sign]
        if @sign_instructions.has_key?(sign_uri)
          @sign_instructions[sign_uri][:instructions] << instructions[index]
        else
          @sign_instructions[sign_uri] = { instructions: Set[instructions[index]], label: sign[:label] }
        end
      end
    end
  end

  def build_data_for_measure(key,rows, measure_signs)
    trafficmeasure_uuid = SecureRandom.uuid
    trafficmeasure_uri = RDF::URI.new "http://data.lblod.info/traffic-measure-concepts/#{trafficmeasure_uuid}"
    RDF::Graph.new do |graph|
      graph.insert([trafficmeasure_uri, RDF.type, LBLOD_MOW.TrafficMeasureConcept])
      graph.insert([trafficmeasure_uri, MU.uuid, trafficmeasure_uuid])
      graph.insert([trafficmeasure_uri, DC.identifier, key])
      measure_signs.each_with_index do |sign, index|
        relation_uuid = SecureRandom.uuid
        relation_uri = RDF::URI.new "http://data.lblod.info/must-use-relations/#{relation_uuid}"
        graph.insert([relation_uri, RDF.type, EXT.MustUseRelation])
        graph.insert([relation_uri, MU.uuid, relation_uuid])
        graph.insert([relation_uri, QB.order, index ])
        graph.insert([relation_uri, EXT.concept, sign[:sign]])
        graph.insert([trafficmeasure_uri, EXT.relation, relation_uri])
      end
      instructions = get_instructions(rows)
      template = build_template(instructions)
      template_uuid = SecureRandom.uuid
      template_uri = RDF::URI.new "http://data.lblod.info/templates/#{template_uuid}"
      graph.insert([template_uri, RDF.type, EXT.Template])
      graph.insert([template_uri, MU.uuid, template_uuid])
      graph.insert([template_uri, EXT.value, template])
      graph.insert([trafficmeasure_uri, EXT.template, template_uri])
      File.write("./output/traffic_measure-#{key}.ttl", graph.dump(:ttl), mode: 'w')
    end
  end

  def build_template(instructions)
    instructions.map{ |instruction| "<p>#{instruction}</p>" }.join("/n")
  end

  def get_instructions(rows)
    instructions = []
    previous_code = ""
    rows.each do |row|
      if previous_code != row["verkeersbord_code"]
        instructions << row["instructie"]
      end
      if row["aanvullende_instructie"]
        instructions << row["aanvullende_instructie"]
      end
      previous_code = row["verkeersbord_code"]
    end
    instructions
  end

  def parse_name(name)
    main_signs = name.split("/")
    signs_to_map = main_signs.map{ |ms| ms.split("+").map{ |ms| ms.split("-") } }.flatten
    signs = []
    signs_to_map.each do |sign|
      signs << map_sign_code_to_uri(sign)
    end
    signs
  end

  def map_sign_code_to_uri_simple(code)
    results = query(%(
SELECT ?sign ?type ?label
WHERE {
  BIND("#{code}" as ?label)
  ?sign rdf:type ?type.
  ?sign skos:prefLabel "#{code}".
}
))
    if results.length === 1
      return results[0]
    else
      return nil
    end
  end

  def map_sign_code_to_uri(code)
    @sign_cache ||= {}
    if @sign_cache.has_key?(code)
      return @sign_cache[code]
    end
    search_code = code.gsub("Type","").strip
    simple_result = map_sign_code_to_uri_simple(search_code)
    if simple_result
      @sign_cache[code]=simple_result
      return simple_result
    end
    results = query(%(
SELECT ?sign ?type ?label
WHERE {
  ?sign rdf:type ?type.
  ?sign skos:prefLabel ?label.
  FILTER (CONTAINS(?label, "#{search_code}"))
}
))
    if results.length === 1
      @sign_cache[code]=results[0]
      return results[0]
    elsif results.length > 1
      puts "#{code} needs to be selected"
      results.each_with_index do |result, index|
        puts "#{index} #{result[:label]}"
      end
      preferred_result = STDIN.gets.chomp.to_i
      puts preferred_result.inspect
      if preferred_result >= 0
        @sign_cache[code] = results[preferred_result]
        return results[preferred_result]
      else
        puts "found no match for #{code}"
        return nil
      end
    else
      @sign_cache[code] = {  }
      puts "found no match for #{code}"
      return nil
    end
  end

  def query(query)
    @client ||= SPARQL::Client.new("http://triplestore:8890/sparql")
    @client.query(query)
  end
end

harvester = InstructieHarvester.new('./input/verkeersmaatregel-templates.csv','./output/sign-mapping.csv')
harvester.harvest
