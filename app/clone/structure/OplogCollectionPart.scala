package clone.structure

import services.{ConfigurationService, MongoService, SectionId}

class OplogCollectionPart(conf: ConfigurationService,
                          collection: Collection,
                          primary: MongoService,
                          secondary: MongoService,
                          startSectionId: SectionId,
                          endSectionId:  SectionId,
                          totalSections:  Int) extends CollectionPart(conf, collection, primary, secondary, startSectionId, endSectionId, totalSections) {

  override def continueFetching(receivedQuantity: Int, expectedQuantity: Int): Boolean = ???

  override def syncSection(offset: Int, limitRead: Int, limitWrite: Int): Unit = ???
}

