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

  # Calculate the total amount spent for a customer
  unless (event.get("visits").nil? || event.get("visits") == "")
    calculate_total("total_spent", "visits", event)
    fav_flavor("favorite_flavor", "visits", event)
  end

  return [event]
end

def calculate_total(segment_name, logstash_ref, event)
  total = 0.0
  seg_len = event.get(logstash_ref).length
  for i in (0..seg_len-1) do
    total += event.get(logstash_ref + "[#{i.to_s}][amount]")
  end
  event.set(segment_name, total)
end

def fav_flavor(segment_name, logstash_ref, event)
  flavor_array = []
  seg_len = event.get(logstash_ref).length
 
  for i in (0..seg_len-1) do
    item_seg_len = event.get(logstash_ref + "[#{i.to_s}][items]").length
    for j in (0..item_seg_len-1) do
      flavor_array.push(event.get(logstash_ref + "[#{i.to_s}][items][#{j.to_s}]"))
    end
  end
  
  # Use max_by to determine which flavor is most popular
  fav_flavor = flavor_array.max_by { |i| flavor_array.count(i) }

  event.set(segment_name, fav_flavor)
end
