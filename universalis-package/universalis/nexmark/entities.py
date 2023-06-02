class Entity:
    def __init__():
        pass
    
    def to_tuple(self):
        return (*self.__dict__.items(), )

class Auction(Entity):
    def __init__(self, id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra):
        self.id = id
        self.itemName = itemName
        self.description = description
        self.initialBid = initialBid
        self.reserve = reserve
        self.dateTime = dateTime
        self.expires = expires
        self.seller = seller
        self.category = category
        self.extra = extra


class Person(Entity):
    def __init__(self, id, name, email, creditCard, city, state, dateTime, extra):
        self.id = id
        self.name = name
        self.emailAddress = email
        self.creditCard = creditCard
        self.city = city
        self.state = state
        self.dateTime = dateTime
        self.extra = extra
        

class Bid(Entity):
    def __init__(self, auction, bidder, price, dateTime, extra):
        self.auction = auction
        self.bidder = bidder
        self.price = price
        self.dateTime = dateTime
        self.extra = extra