# register accespts the hashmap passed to "script_params" - runs at startup
def register(params)
end

# filter runs for each event
# return the list of events to be passed forward
def filter(event)

  # Map of cassandra table names to logstash reference names
  field_hash = {
      "name"				=> "[parsed_message][customers][name]",
      "id"				=> "[parsed_message][customers][id]",
      "address"				=> "[parsed_message][customers][address]",
      "visits"				=> "[parsed_message][customers][visits]" }

  # Loop over the above list of fields
  # Set fields if they are not null or ""
  field_hash.each do |name, ref|
    unless (event.get(ref).nil? || event.get(ref) == "")
      event.set(name, event.get(ref))
    end
  end 

  # Cut connexus key into two parts
  # Check to make sure connexus key exists
  #unless (event.get("connexuskey").nil? || event.get("connexuskey") == "") 
  #  event.set('f7connexuskey', event.get('connexuskey')[0..6])
  #  event.set('l5connexuskey', event.get('connexuskey')[-5..-1])
  #end

  # Loop over each address element to create the full address
  #ad_array = [ "ad_primary", "ad_street", "ad_street_type", "ad_street_dir", "ad_city", "ad_state", "ad_zip" ]
  #full_address = ""
  #ad_array.each do |ref|
  #  unless (event.get(ref).nil? || event.get(ref) == "")
  #    full_address += event.get(ref)
  #    if ref != "ad_zip"
  #      full_address += " "
  #    end
  #  end
  #end
  #event.set('address', full_address)

  # Primary keys to cassandra table 1
  # Need to be set for INSERT
  #primary_keys = [ "f7connexuskey", "rollover", "l5connexuskey", "lname", "fname", "ssn", "address", "phone_number"]
  #primary_keys.each do |ref|
  #  if event.get(ref).nil? || event.get(ref) == ""
      # If any of the primary keys are null or empty, we drop this record
  #    return []
  #  end
  #end

  # Map of cassandra table names to logstash reference names from JSON parsing
  # Each of these segments can be an array
  #segments_hash = { "employment_segment"    => "[parsed_message][CreditHeader][Employment_Segment]",
  #                  "otherid_segment"       => "[parsed_message][CreditHeader][OtherId_Segment]",
  #                  "bankruptcy_segment"    => "[parsed_message][CreditHeader][Bankruptcy_Segment]",
  #                  "collection_segment"    => "[parsed_message][CreditHeader][Collection_Segment]",
  #                  "legalitem_segment"     => "[parsed_message][CreditHeader][LegalItem_Segment]",
  #                  "taxlien_segment"       => "[parsed_message][CreditHeader][TaxLien_Segment]",
  #                  "alertcontact_segment"  => "[parsed_message][CreditHeader][AlertContact_Segment]", 
  #                  "consumerblock_segment" => "[parsed_message][CreditHeader][ConsumerBlock_Segment]",
  #                  "forinq_segment"        => "[parsed_message][CreditHeader][ForInq_Segment]",
  #                  "stdinq_segment"        => "[parsed_message][CreditHeader][StdInq_Segment]",
  #                  "locate_segment"        => "[parsed_message][CreditHeader][Locate_Segment]",
  #                  "model_segment"         => "[parsed_message][CreditHeader][Model_Segment]" }

  # Split each of the arrays in the above table
  # This will create an array of strings in cassandra for each column
  # Ex. employment_segment = ['{"SegmentType":"EM", "PostedOrLeftDa...}', '{"SegmentType":"EM"...}']
  #segments_hash.each do |name, ref|
  #  unless event.get(ref).nil?
  #    split_segment(name,ref,event)
  #  end
  #end

  # create the trades file when the Trade_Segment exists
  #unless (event.get("[parsed_message][CreditHeader][Trade_Segment]").nil? || event.get("[parsed_message][CreditHeader][Trade_Segment]") == "")
  #  create_trades_file(event)
  #end

  # return the filtered event back to logstash
  return [event]
end

def split_segment(segment_name, logstash_ref, event)
  # create empty list to append elements
  ret_list = []
  # determine array length
  seg_len = event.get(logstash_ref).length
  for i in (0..seg_len-1) do
    ret_list.push(event.get(logstash_ref + "[#{i.to_s}]").to_s.gsub("=>",":"))
  end
  # set the cassandra column to the return array
  event.set(segment_name, ret_list)
end

def create_trades_file(event) 
    # Generate a filename for the trade segment
    # Run SHA256 on fname, lname, ssn, and trade segment
    trades_filename = "/tmp/" + (Digest::SHA256.hexdigest (event.get("fname") + event.get("lname") + event.get("ssn").to_s + event.get("[parsed_message][CreditHeader][Trade_Segment]").to_s)).to_s
    event.set("trades_file", trades_filename)
	  
    # Write to trades file
    # TRades file contains a string of all the trades (replaces "=>" with ":")
    trades_file = File.open(trades_filename, "w")
    trades_file.write(event.get("[parsed_message][CreditHeader][Trade_Segment]").to_s.gsub("=>",":"))
    trades_file.close
end
