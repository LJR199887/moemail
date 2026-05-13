import { createDb } from "@/lib/db"
import { and, eq, gt, inArray, lt, or, sql } from "drizzle-orm"
import { NextResponse } from "next/server"
import { emailShares, emails, messages, messageShares } from "@/lib/schema"
import { encodeCursor, decodeCursor } from "@/lib/cursor"
import { getUserId } from "@/lib/apiKey"

export const runtime = "edge"

const PAGE_SIZE = 20
const DELETE_BATCH_SIZE = 100

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size))
  }
  return chunks
}

function isMissingTableError(error: unknown) {
  return error instanceof Error && /no such table/i.test(error.message)
}

async function ignoreMissingTable(operation: unknown) {
  try {
    await operation
  } catch (error) {
    if (!isMissingTableError(error)) {
      throw error
    }
    console.warn('Skipping cleanup for missing table:', error)
  }
}

function getErrorMessage(error: unknown, fallback: string) {
  return error instanceof Error && error.message ? error.message : fallback
}

export async function DELETE(request: Request) {
  const userId = await getUserId()

  try {
    const db = createDb()
    const { ids } = await request.json<{ ids?: string[] }>()
    const uniqueIds = Array.from(new Set(ids || [])).filter(Boolean)

    if (uniqueIds.length === 0) {
      return NextResponse.json(
        { error: "No emails selected" },
        { status: 400 }
      )
    }

    const ownedEmailIds: string[] = []
    for (const idChunk of chunkArray(uniqueIds, DELETE_BATCH_SIZE)) {
      const ownedEmails = await db.query.emails.findMany({
        where: and(
          eq(emails.userId, userId!),
          inArray(emails.id, idChunk)
        ),
        columns: { id: true }
      })
      ownedEmailIds.push(...ownedEmails.map(email => email.id))
    }

    if (ownedEmailIds.length === 0) {
      return NextResponse.json(
        { error: "No matching emails found" },
        { status: 404 }
      )
    }

    const messageIds: string[] = []
    for (const emailIdChunk of chunkArray(ownedEmailIds, DELETE_BATCH_SIZE)) {
      const emailMessages = await db.query.messages.findMany({
        where: inArray(messages.emailId, emailIdChunk),
        columns: { id: true }
      })
      messageIds.push(...emailMessages.map(message => message.id))
    }

    for (const messageIdChunk of chunkArray(messageIds, DELETE_BATCH_SIZE)) {
      await ignoreMissingTable(
        db.delete(messageShares)
          .where(inArray(messageShares.messageId, messageIdChunk))
      )
    }

    for (const emailIdChunk of chunkArray(ownedEmailIds, DELETE_BATCH_SIZE)) {
      await ignoreMissingTable(
        db.delete(emailShares)
          .where(inArray(emailShares.emailId, emailIdChunk))
      )

      await db.delete(messages)
        .where(inArray(messages.emailId, emailIdChunk))

      await db.delete(emails)
        .where(inArray(emails.id, emailIdChunk))
    }

    return NextResponse.json({ success: true, deleted: ownedEmailIds.length })
  } catch (error) {
    console.error('Failed to delete emails:', error)
    return NextResponse.json(
      { error: getErrorMessage(error, "Failed to delete emails") },
      { status: 500 }
    )
  }
}

export async function GET(request: Request) {
  const userId = await getUserId()

  const { searchParams } = new URL(request.url)
  const cursor = searchParams.get('cursor')
  const sort = searchParams.get('sort') === 'createdAt:asc' ? 'asc' : 'desc'

  const db = createDb()

  try {
    const baseConditions = and(
      eq(emails.userId, userId!),
      gt(emails.expiresAt, new Date())
    )

    const totalResult = await db.select({ count: sql<number>`count(*)` })
      .from(emails)
      .where(baseConditions)
    const totalCount = Number(totalResult[0].count)

    const conditions = [baseConditions]

    if (cursor) {
      const { timestamp, id } = decodeCursor(cursor)
      const cursorDate = new Date(timestamp)
      conditions.push(
        sort === 'asc'
          ? or(
              gt(emails.createdAt, cursorDate),
              and(
                eq(emails.createdAt, cursorDate),
                gt(emails.id, id)
              )
            )
          : or(
              lt(emails.createdAt, cursorDate),
              and(
                eq(emails.createdAt, cursorDate),
                lt(emails.id, id)
              )
            )
      )
    }
    const results = await db.query.emails.findMany({
      where: and(...conditions),
      orderBy: (emails, { asc, desc }) => sort === 'asc'
        ? [asc(emails.createdAt), asc(emails.id)]
        : [desc(emails.createdAt), desc(emails.id)],
      limit: PAGE_SIZE + 1
    })

    const hasMore = results.length > PAGE_SIZE
    const nextCursor = hasMore
      ? encodeCursor(
          results[PAGE_SIZE - 1].createdAt.getTime(),
          results[PAGE_SIZE - 1].id
        )
      : null
    const emailList = hasMore ? results.slice(0, PAGE_SIZE) : results

    return NextResponse.json({
      emails: emailList,
      nextCursor,
      total: totalCount
    })
  } catch (error) {
    console.error('Failed to fetch user emails:', error)
    return NextResponse.json(
      { error: "Failed to fetch emails" },
      { status: 500 }
    )
  }
}
